# fake115uploader
模拟115网盘客户端的上传功能，部分实现参考 [Fake115Upload](https://github.com/T3rry7f/Fake115Upload)，仅用于研究目的

### 特点
* 支持秒传模式，需要已经有人上传过指定文件到115
* 支持上传超大文件，上传大小超过5GB的文件需要115 vip会员
* 支持分片模式上传，分片并行上传（适合用于上传超大文件）
* 支持多个上传任务同时进行
* 支持显示上传进度条

### 编译安装
安装最新稳定版本，请用最新版本的Go运行：

`go install github.com/orzogc/fake115uploader@latest`

安装最新git版本，请用最新版本的Go运行：

`go install github.com/orzogc/fake115uploader@master`

### 使用方法
首先要先运行一次 `fake115uploader` 生成设置文件fake115uploader.json（使用 `-l 文件` 指定设置文件，默认为程序所在的文件夹里的fake115uploader.json），然后登陆网页版115，按F12后刷新，将115网页请求的Cookie的值全部复制到fake115uploader.json的cookies的值里（参考[这里](https://github.com/LSD08KM/Fake115Upload_Python3#cookies%E5%9C%A8%E5%93%AA%E9%87%8C)），或者运行时用参数 `-k Cookie` 指定要用的Cookie。

`fake115uploader -f 文件` 秒传模式上传文件，可以指定多个文件且文件必须是最后一个参数，下同。

`fake115uploader -u 文件` 先尝试用秒传模式上传文件，失败后改用普通模式上传，不支持上传超过5GB的文件。

`fake115uploader -m 文件` 先尝试用秒传模式上传文件，失败后改用分片模式上传（适合用于上传超大文件）。默认根据文件大小自动分片，每片128MB，分片并行上传，每个文件默认最多4个分片同时上传。可以设置fake115uploader.json的partSizeMB或者用 `-part-size 分片大小` 参数指定分片大小（单位MB，范围为1到5120）；设置fake115uploader.json的parallelParts或者用 `-parallel-parts 并行分片数` 参数指定每个文件的最大并行分片数（范围为1到100）；设置fake115uploader.json的partsNum或者用 `-parts-num 分片数量` 参数指定上传文件的分片数量（数量范围为1到10000，设置后忽略分片大小）。

支持多个上传任务同时进行，默认同时进行2个任务。可以设置fake115uploader.json的concurrentUploads或者用 `-concurrent-uploads 任务数` 参数指定同时上传的最大任务数（范围为1到10）。

要上传文件到指定的115文件夹，可以在fake115uploader.json或运行时加上参数 `-c cid` 设置cid（参数设置会覆盖设置文件里的设置，默认为0，即根目录），cid为115文件夹的cid，可以登陆115网页版查看网页地址获取cid。

要上传文件夹，需要运行时加上参数 `-recursive` 。

设置fake115uploader.json的resultDir或运行时加上参数 `-r 文件夹` 可以将上传结果保存在指定的文件夹内，默认不保存。

运行时加上参数 `-n` 不读取设置文件，这时必须要用 `-k Cookie` 指定115的Cookie。

上传文件时加上参数 `-a` 利用阿里云内网上传文件，需要在阿里云服务器上运行本程序，同时也需要115在服务器的所在地域开通了阿里云OSS，可以在服务器上运行 `curl https://uplb.115.com/3.0/getuploadinfo.php` 查看OSS地域。

上传文件时加上参数 `-e` ，上传成功后自动删除本地原文件。

设置fake115uploader.json的httpRetry或运行时加上参数 `-http-retry 重试次数` 设置HTTP请求失败后的重试次数，默认为0（即不重试）。

运行时加上参数 `-v` 显示更详细的信息（调试用）。

### 代理设置
`fake115uploader`的HTTP请求和OSS上传默认使用环境变量`http_proxy`和`https_proxy`的值作为代理。

可以设置fake115uploader.json的httpProxy或者使用参数`-http-proxy 代理`设置HTTP代理，支持SOCKS5代理。

可以设置fake115uploader.json的ossProxy或者使用参数`-oss-proxy 代理`设置OSS上传代理，代理格式和HTTP代理一致，不支持SOCKS5代理。
