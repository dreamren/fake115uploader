package main

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fastjson"
)

/*
type cbackVar struct {
	PickCode     string `json:"x:pick_code"`
	UserID       string `json:"x:user_id"`
	BehaviorType string `json:"x:behavior_type"`
	Source       string `json:"x:source"`
	Target       string `json:"x:target"`
}
*/

type callback struct {
	Callback    string `json:"callback"`
	CallbackVar string `json:"callback_var"`
}

type fastToken struct {
	Request    string   `json:"request"`
	Status     int      `json:"status"`
	StatusCode int      `json:"statuscode"`
	StatusMsg  string   `json:"statusmsg"`
	PickCode   string   `json:"pickcode"`
	Target     string   `json:"target"`
	Version    string   `json:"version"`
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	Callback   callback `json:"callback"`
	SHA1       string   // 文件的 sha1 hash 值
}

const md5Salt = "Qclm8MGWUv59TnrR0XPg"

// 上传 SHA1 的值到 115
func uploadSHA1(filename, fileSize, totalHash, signKey, signVal string, targetCID uint64) (body []byte, e error) {
	fileID := strings.ToUpper(totalHash)
	target := targetPrefix + strconv.FormatUint(targetCID, 10)
	data := sha1.Sum([]byte(userID + fileID + target + "0"))
	hash := hex.EncodeToString(data[:])
	sigStr := userKey + hash + endString
	data = sha1.Sum([]byte(sigStr))
	sig := strings.ToUpper(hex.EncodeToString(data[:]))

	t := time.Now().Unix()

	userIdMd5 := md5.Sum([]byte(userID))
	tokenMd5 := md5.Sum([]byte(md5Salt + fileID + fileSize + signKey + signVal + userID + strconv.FormatInt(t, 10) + hex.EncodeToString(userIdMd5[:]) + appVer))
	token := hex.EncodeToString(tokenMd5[:])

	encodedToken, err := ecdhCipher.EncodeToken(t)
	if err != nil {
		return nil, fmt.Errorf("加密 token 出现错误：%w", err)
	}

	uploadURL := fmt.Sprintf(initURL, encodedToken)

	if *verbose {
		log.Printf("initupload的链接是：%s", uploadURL)
		log.Printf("sig的值是：%s", sig)
		log.Printf("token的值是：%s", token)
		log.Printf("k_ec的值是：%s", encodedToken)
	}

	form := url.Values{}
	form.Set("appid", "0")
	form.Set("appversion", appVer)
	form.Set("userid", userID)
	form.Set("filename", filename)
	form.Set("filesize", fileSize)
	form.Set("fileid", fileID)
	form.Set("target", target)
	form.Set("sig", sig)
	form.Set("t", strconv.FormatInt(t, 10))
	form.Set("token", token)
	if signKey != "" && signVal != "" {
		form.Set("sign_key", signKey)
		form.Set("sign_val", signVal)
	}

	encrypted, err := ecdhCipher.Encrypt([]byte(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("加密上传请求出现错误：%w", err)
	}

	req, err := http.NewRequest(http.MethodPost, uploadURL, bytes.NewReader(encrypted))
	if err != nil {
		return nil, fmt.Errorf("构造 initupload 请求出现错误：%w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cookie", config.Cookies)
	resp, err := doRequest(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 initupload 的响应出现错误：%w", err)
	}
	decrypted, err := ecdhCipher.Decrypt(body)
	if err != nil {
		if *verbose {
			log.Printf("解密响应体出现错误：%v", err)
		}

		return body, nil
	}

	return decrypted, nil
}

// 利用文件的 sha1 hash 值上传文件获取响应
func (file *fileInfo) uploadFileSHA1(slot *progSlot) (body []byte, fileSHA1 string, e error) {
	f, err := os.Open(file.Path)
	if err != nil {
		return nil, "", fmt.Errorf("打开 %s 出现错误：%w", file.Path, err)
	}
	defer f.Close()

	_, totalHash, err := hashSHA1(f, file.Name, slot)
	if err != nil {
		return nil, "", err
	}

	info, err := os.Stat(file.Path)
	if err != nil {
		return nil, "", fmt.Errorf("获取 %s 的信息出现错误：%w", file.Path, err)
	}
	filename := info.Name()
	fileSize := strconv.FormatInt(info.Size(), 10)
	targetCID := file.ParentID

	body, err = uploadSHA1(filename, fileSize, totalHash, "", "", targetCID)
	if err != nil {
		return nil, "", err
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil, "", fmt.Errorf("解析 %s 的秒传响应出现错误：%w", file.Path, err)
	}
	if v.GetInt("status") == 7 && v.GetInt("statuscode") == 701 {
		if *verbose {
			log.Printf("秒传模式上传文件 %s 的响应体的内容是：\n%s", file.Path, string(body))
		}

		signKey := string(v.GetStringBytes("sign_key"))
		signCheck := string(v.GetStringBytes("sign_check"))
		signVal, err := hashFileRange(f, signCheck)
		if err != nil {
			return nil, "", err
		}

		body, err = uploadSHA1(filename, fileSize, totalHash, signKey, signVal, targetCID)
		if err != nil {
			return nil, "", err
		}
	}

	return body, totalHash, nil
}

// 以秒传模式上传文件，秒传失败时返回的 token 供普通模式和分片模式使用
// 秒传失败是常态（新文件基本都秒传不中），失败时不打印日志
func (file *fileInfo) fastUploadFile(slot *progSlot) (token *fastToken, e error) {
	token = new(fastToken)

	body, fileSHA1, err := file.uploadFileSHA1(slot)
	if err != nil {
		return nil, err
	}
	token.SHA1 = fileSHA1

	if *verbose {
		log.Printf("秒传模式上传文件 %s 的响应体的内容是：\n%s", file.Path, string(body))
	}

	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("解析 %s 的秒传响应出现错误：%w", file.Path, err)
	}
	if v.GetInt("status") == 2 && v.Exists("statuscode") && v.GetInt("statuscode") == 0 {
		// 小于 1MB 的文件只在汇总中显示；大文件秒传成功值得单独提示（跳过了整个上传过程）
		if info, serr := os.Stat(file.Path); serr == nil && info.Size() >= minBarSize {
			log.Printf("秒传模式上传 %s 成功", file.Name)
		}
		if *removeFile {
			if err = remove(file.Path); err != nil {
				return nil, err
			}
		}
	} else if v.GetInt("status") == 1 && v.Exists("statuscode") && v.GetInt("statuscode") == 0 {
		// 秒传失败的响应包含普通上传模式和分片上传模式的 token
		if err = json.Unmarshal(body, &token); err != nil {
			return nil, fmt.Errorf("解析 %s 的秒传响应出现错误：%w", file.Path, err)
		}

		if *verbose {
			log.Printf("秒传模式上传 %s 失败返回的内容是：\n%+v", file.Path, token)
		}

		return token, fmt.Errorf("秒传模式上传 %s 失败", file.Path)
	} else {
		return nil, fmt.Errorf("秒传模式上传 %s 失败", file.Path)
	}

	return token, nil
}
