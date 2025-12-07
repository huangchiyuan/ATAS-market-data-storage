# 仓库设置指南（快速版）

> 📖 **详细版本请查看 [GITHUB_SETUP.md](GITHUB_SETUP.md)**

## 快速步骤

### 1. 在 GitHub 上创建新仓库

1. 访问 https://github.com 并登录
2. 点击右上角 **"+"** → **"New repository"**
3. 填写信息：
   - Repository name: `market-data-storage`
   - Description: `高性能市场数据接收、存储和读取系统`
   - 选择 Public 或 Private
   - ⚠️ **不要**勾选 README、.gitignore、LICENSE
4. 点击 **"Create repository"**
5. 复制仓库 HTTPS 地址（例如：`https://github.com/YOUR_USERNAME/market-data-storage.git`）

### 2. 初始化并推送代码

```bash
# 进入项目目录
cd market_data_storage

# 初始化 Git
git init

# 添加所有文件
git add .

# 创建初始提交
git commit -m "Initial commit: Market Data Storage System"

# 添加远程仓库（替换为你的仓库地址）
git remote add origin https://github.com/YOUR_USERNAME/market-data-storage.git

# 设置主分支并推送
git branch -M main
git push -u origin main
```

### 3. 验证

访问你的 GitHub 仓库页面，确认所有文件都已上传。

---

## 详细说明

如需更详细的步骤说明、常见问题解答、Token 配置等，请查看 **[GITHUB_SETUP.md](GITHUB_SETUP.md)**

