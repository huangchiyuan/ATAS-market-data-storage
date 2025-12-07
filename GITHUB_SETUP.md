# GitHub 仓库创建和推送指南

## 📋 完整步骤

### 第一步：在 GitHub 上创建新仓库

1. **登录 GitHub**
   - 访问 https://github.com
   - 登录你的账户

2. **创建新仓库**
   - 点击右上角的 **"+"** 按钮
   - 选择 **"New repository"**

3. **填写仓库信息**
   - **Repository name**: `market-data-storage`（或你喜欢的名字）
   - **Description**: `高性能市场数据接收、存储和读取系统 - 支持 ATAS 平台数据存储到 DuckDB`
   - **Visibility**: 
     - 选择 **Public**（公开，任何人都能看到）
     - 或选择 **Private**（私有，只有你能看到）
   - ⚠️ **重要**：**不要**勾选以下选项：
     - ❌ Add a README file（我们已经有了）
     - ❌ Add .gitignore（我们已经有了）
     - ❌ Choose a license（我们已经有了）
   - 点击 **"Create repository"** 按钮

4. **复制仓库地址**
   - 创建成功后，GitHub 会显示仓库页面
   - 点击绿色的 **"Code"** 按钮
   - 复制 HTTPS 地址，例如：`https://github.com/YOUR_USERNAME/market-data-storage.git`
   - 保存这个地址，下一步会用到

---

### 第二步：初始化本地 Git 仓库

1. **打开命令行（PowerShell 或 CMD）**

2. **进入项目目录**
   ```bash
   cd market_data_storage
   ```

3. **初始化 Git 仓库**
   ```bash
   git init
   ```
   输出应该显示：`Initialized empty Git repository in ...`

4. **添加所有文件**
   ```bash
   git add .
   ```
   这会添加所有文件到暂存区（除了 .gitignore 中排除的文件）

5. **创建初始提交**
   ```bash
   git commit -m "Initial commit: Market Data Storage System"
   ```
   输出应该显示类似：`[main (root-commit) xxxxx] Initial commit...`

---

### 第三步：连接远程仓库并推送

1. **添加远程仓库**
   ```bash
   git remote add origin https://github.com/YOUR_USERNAME/market-data-storage.git
   ```
   ⚠️ **注意**：将 `YOUR_USERNAME` 替换为你的 GitHub 用户名，`market-data-storage` 替换为你创建的仓库名

2. **设置主分支名称（如果需要）**
   ```bash
   git branch -M main
   ```
   这会将当前分支重命名为 `main`（GitHub 的默认分支名）

3. **推送到 GitHub**
   ```bash
   git push -u origin main
   ```

4. **输入 GitHub 凭证**
   - 如果提示输入用户名和密码：
     - **用户名**：你的 GitHub 用户名
     - **密码**：使用 **Personal Access Token**（不是 GitHub 密码）
     - 如果还没有 Token，见下方说明

---

### 第四步：验证推送结果

1. **刷新 GitHub 仓库页面**
   - 你应该能看到所有文件都已上传
   - 包括：README.md、Python 文件、C# 文件等

2. **检查文件列表**
   - ✅ README.md
   - ✅ requirements.txt
   - ✅ LICENSE
   - ✅ .gitignore
   - ✅ data_storage_module.py
   - ✅ data_reader_for_backtest.py
   - ✅ csharp/ 目录
   - ✅ 其他文档文件

---

## 🔐 GitHub Personal Access Token（如果需要）

如果 `git push` 时提示需要密码，你需要使用 Personal Access Token：

### 创建 Token

1. **登录 GitHub** → 点击右上角头像 → **Settings**

2. **进入 Developer settings**
   - 滚动到页面底部
   - 点击左侧菜单的 **"Developer settings"**

3. **创建 Token**
   - 点击 **"Personal access tokens"** → **"Tokens (classic)"**
   - 点击 **"Generate new token"** → **"Generate new token (classic)"**

4. **配置 Token**
   - **Note**: 输入描述，如 "Market Data Storage"
   - **Expiration**: 选择过期时间（建议选择较长时间）
   - **Scopes**: 勾选 **`repo`**（完整仓库访问权限）
   - 点击 **"Generate token"**

5. **复制 Token**
   - ⚠️ **重要**：Token 只显示一次，立即复制保存
   - 格式类似：`ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

6. **使用 Token**
   - 当 `git push` 提示输入密码时，粘贴这个 Token（不是 GitHub 密码）

---

## 📝 后续更新代码

当你修改了代码，需要推送到 GitHub 时：

```bash
# 1. 查看更改状态
git status

# 2. 添加更改的文件
git add .

# 或添加特定文件
git add README.md data_storage_module.py

# 3. 提交更改
git commit -m "描述你的更改内容"

# 4. 推送到 GitHub
git push
```

---

## 🔍 常见问题

### Q1: 提示 "remote origin already exists"

**A**: 说明已经添加过远程仓库，可以：
```bash
# 查看现有远程仓库
git remote -v

# 如果需要修改，先删除再添加
git remote remove origin
git remote add origin https://github.com/YOUR_USERNAME/market-data-storage.git
```

### Q2: 提示 "failed to push some refs"

**A**: 可能是远程仓库有文件而本地没有，可以：
```bash
# 先拉取远程更改
git pull origin main --allow-unrelated-histories

# 然后再推送
git push -u origin main
```

### Q3: 忘记添加某些文件

**A**: 
```bash
# 添加文件
git add 文件名

# 提交
git commit -m "添加文件"

# 推送
git push
```

### Q4: 想撤销最后一次提交

**A**:
```bash
# 撤销提交但保留更改
git reset --soft HEAD~1

# 或完全撤销（删除更改）
git reset --hard HEAD~1
```

---

## ✅ 完成检查清单

- [ ] 在 GitHub 上创建了新仓库
- [ ] 复制了仓库 HTTPS 地址
- [ ] 在项目目录中运行了 `git init`
- [ ] 运行了 `git add .`
- [ ] 运行了 `git commit -m "Initial commit"`
- [ ] 运行了 `git remote add origin <仓库地址>`
- [ ] 运行了 `git push -u origin main`
- [ ] 在 GitHub 上验证了所有文件都已上传

---

**完成！** 🎉 现在你的代码已经在 GitHub 上了！

