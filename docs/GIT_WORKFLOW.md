# Git Workflow Guide

## 🌳 Branch Strategy

### Branch Types

```
main
  ├── Production releases
  └── Tagged versions (v1.0.0, v1.1.0, etc.)

dev
  ├── Integration branch for features
  └── Pre-release testing

feature/*
  ├── New features
  └── Merge into dev

bugfix/*
  ├── Bug fixes
  └── Merge into dev

hotfix/*
  ├── Urgent production fixes
  └── Merge into main and dev

release/*
  ├── Release preparation
  └── Merge into main and dev
```

---

## 🚀 Quick Start

### Initial Setup

```bash
# Clone repository
git clone https://github.com/HasnainShafiq98/databricks-insight-agent.git
cd databricks-insight-agent

# Create dev branch if it doesn't exist
git checkout -b dev
git push -u origin dev

# Verify branches
git branch -a
```

---

## 📝 Daily Workflow

### Starting a New Feature

```bash
# 1. Update dev branch
git checkout dev
git pull origin dev

# 2. Create feature branch
git checkout -b feature/add-metric-dashboard

# 3. Verify you're on the right branch
git status
```

### Making Changes

```bash
# 1. Make your changes
# Edit files...

# 2. Check what changed
git status
git diff

# 3. Stage changes
git add .
# Or add specific files
git add agent.py sql_generator.py

# 4. Commit with descriptive message
git commit -m "feat(dashboard): Add customer metrics dashboard

- Implement customer retention calculation
- Add churn rate visualization
- Update documentation

Closes #42"

# 5. Push to remote
git push origin feature/add-metric-dashboard
```

### Creating a Pull Request

1. Go to GitHub repository
2. Click "Compare & pull request"
3. Set base branch to `dev`
4. Fill in PR template:

```markdown
## Description
Add customer metrics dashboard with retention and churn calculations

## Type of Change
- [x] New feature
- [ ] Bug fix
- [ ] Documentation update
- [ ] Refactoring

## Testing
- [x] Unit tests added/updated
- [x] Integration tests passed
- [x] Manual testing completed

## Screenshots (if applicable)
[Add screenshots here]

## Checklist
- [x] Code follows style guidelines
- [x] Documentation updated
- [x] No breaking changes
- [x] Tests passing
```

5. Request review from team members
6. Address review comments
7. Merge when approved

---

## 🔄 Keeping Feature Branch Updated

```bash
# While working on feature, regularly sync with dev
git checkout dev
git pull origin dev
git checkout feature/your-feature
git merge dev

# Or use rebase for cleaner history
git checkout feature/your-feature
git rebase dev

# Resolve any conflicts
# Edit conflicted files
git add .
git rebase --continue

# Force push if you rebased
git push origin feature/your-feature --force-with-lease
```

---

## 🏷️ Release Process

### Preparing a Release

```bash
# 1. Create release branch from dev
git checkout dev
git pull origin dev
git checkout -b release/v1.2.0

# 2. Update version numbers
# Edit version in:
# - README.md
# - CHANGELOG.md
# - Any version files

# 3. Commit version bump
git add .
git commit -m "chore: Bump version to 1.2.0"

# 4. Push release branch
git push origin release/v1.2.0
```

### Finalizing Release

```bash
# 1. Merge to main
git checkout main
git pull origin main
git merge release/v1.2.0
git push origin main

# 2. Tag the release
git tag -a v1.2.0 -m "Release version 1.2.0

Features:
- Add customer metrics dashboard
- Improve SQL error correction
- Add document chunking optimization

Bug Fixes:
- Fix FAISS index persistence
- Resolve rate limiting edge cases

Documentation:
- Update setup guide
- Add API documentation"

git push origin v1.2.0

# 3. Merge back to dev
git checkout dev
git merge release/v1.2.0
git push origin dev

# 4. Delete release branch
git branch -d release/v1.2.0
git push origin --delete release/v1.2.0
```

---

## 🚨 Hotfix Process

For urgent production fixes:

```bash
# 1. Create hotfix from main
git checkout main
git pull origin main
git checkout -b hotfix/security-patch

# 2. Make the fix
# Edit files...

# 3. Commit fix
git add .
git commit -m "fix(security): Patch SQL injection vulnerability

Critical security fix to prevent SQL injection through
ORDER BY clause parameters.

CVE-2026-XXXX"

# 4. Test thoroughly
pytest
python example_usage.py

# 5. Merge to main
git checkout main
git merge hotfix/security-patch
git push origin main

# 6. Tag hotfix version
git tag -a v1.1.1 -m "Hotfix v1.1.1: Security patch"
git push origin v1.1.1

# 7. Merge to dev
git checkout dev
git merge hotfix/security-patch
git push origin dev

# 8. Delete hotfix branch
git branch -d hotfix/security-patch
git push origin --delete hotfix/security-patch
```

---

## 📋 Commit Message Guidelines

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, no logic change)
- `refactor`: Code refactoring
- `perf`: Performance improvement
- `test`: Adding tests
- `chore`: Build process or tooling
- `revert`: Revert previous commit

### Examples

**Feature:**
```bash
git commit -m "feat(sql): Add support for subqueries

Implement nested SELECT statements in SQL generator
with proper validation and security checks.

Closes #123"
```

**Bug Fix:**
```bash
git commit -m "fix(context): Resolve FAISS index loading error

Fixed issue where FAISS index wouldn't load from
DBFS due to incorrect path handling.

Fixes #456"
```

**Documentation:**
```bash
git commit -m "docs: Update API documentation

Add examples for all public methods in agent.py
and improve inline code comments."
```

**Refactoring:**
```bash
git commit -m "refactor(pipeline): Simplify Delta table creation

Extract common logic into helper methods and
improve code readability without changing behavior."
```

---

## 🛠️ Useful Git Commands

### Viewing History

```bash
# View commit history
git log --oneline --graph --all

# View changes in last 3 commits
git log -3 -p

# View commits by author
git log --author="username"

# Search commits by message
git log --grep="feature"
```

### Undoing Changes

```bash
# Undo unstaged changes
git checkout -- filename.py

# Undo staged changes
git reset HEAD filename.py

# Undo last commit (keep changes)
git reset --soft HEAD~1

# Undo last commit (discard changes)
git reset --hard HEAD~1

# Undo pushed commit (create new commit)
git revert HEAD
git push
```

### Stashing

```bash
# Save current work temporarily
git stash save "WIP: working on feature"

# List stashes
git stash list

# Apply most recent stash
git stash apply

# Apply and remove stash
git stash pop

# Drop specific stash
git stash drop stash@{0}

# View stash contents
git stash show -p stash@{0}
```

### Cleaning Up

```bash
# Remove untracked files (dry run)
git clean -n

# Remove untracked files
git clean -f

# Remove untracked files and directories
git clean -fd

# Remove ignored files too
git clean -fdx
```

### Branch Management

```bash
# List all branches
git branch -a

# Delete local branch
git branch -d feature/old-feature

# Delete remote branch
git push origin --delete feature/old-feature

# Rename current branch
git branch -m new-branch-name

# Prune deleted remote branches
git fetch --prune
```

---

## 🔍 Code Review Checklist

### For Authors

Before creating PR:
- [ ] Code follows project style guidelines
- [ ] All tests pass
- [ ] New tests added for new functionality
- [ ] Documentation updated
- [ ] No debugging code or console.logs
- [ ] Commit messages are clear and descriptive
- [ ] Branch is up-to-date with dev

### For Reviewers

When reviewing PR:
- [ ] Code is clear and maintainable
- [ ] Logic is correct and handles edge cases
- [ ] No security vulnerabilities
- [ ] Performance considerations addressed
- [ ] Tests are comprehensive
- [ ] Documentation is accurate
- [ ] No unnecessary complexity

---

## 📊 Changelog Maintenance

Update CHANGELOG.md with each release:

```markdown
# Changelog

## [1.2.0] - 2026-01-15

### Added
- Customer metrics dashboard with retention analysis
- Document chunking optimization for better RAG results
- Auto-retry logic for transient database errors

### Changed
- Improved SQL error correction accuracy
- Updated FAISS index structure for faster search
- Refactored agent decision logic

### Fixed
- Fixed FAISS index persistence in DBFS
- Resolved rate limiting edge cases
- Corrected schema validation for complex queries

### Security
- Added input validation for ORDER BY parameters
- Enhanced SQL injection protection

## [1.1.0] - 2026-01-01
...
```

---

## 🎯 Best Practices

### DO

✅ Commit frequently with meaningful messages  
✅ Keep commits focused and atomic  
✅ Write descriptive PR descriptions  
✅ Test before pushing  
✅ Keep branches up-to-date with dev  
✅ Delete merged branches  
✅ Use feature flags for large changes  
✅ Document breaking changes

### DON'T

❌ Commit directly to main  
❌ Push unfinished code to shared branches  
❌ Use vague commit messages ("fix stuff", "update")  
❌ Include credentials or secrets  
❌ Commit generated files (logs, cache)  
❌ Force push to shared branches (except after rebase)  
❌ Leave branches stale for weeks  
❌ Mix multiple features in one PR

---

## 🔐 Security

### Secrets Management

```bash
# NEVER commit these:
.env
*.key
*.pem
*_credentials.json

# Verify .gitignore
cat .gitignore | grep -E "\.env|\.key|credentials"

# Check for accidentally committed secrets
git log -p | grep -i "password\|token\|secret"

# Remove secret from history
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch path/to/secret' \
  --prune-empty --tag-name-filter cat -- --all
```

---

## 📚 Resources

- [Git Documentation](https://git-scm.com/doc)
- [GitHub Flow Guide](https://guides.github.com/introduction/flow/)
- [Conventional Commits](https://www.conventionalcommits.org/)
- [Semantic Versioning](https://semver.org/)

---

**Last Updated:** January 11, 2026  
**Version:** 2.0.0
