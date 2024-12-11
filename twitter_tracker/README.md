# Twitter 追踪工具

## 项目状态
- [x] 基础框架搭建完成
- [x] MongoDB数据库配置完成
- [ ] Twitter API配置 (待添加)

## 已完成工作

### 1. 代码结构
- `twitter_api.py`: Twitter API交互核心类
- `cli.py`: 命令行接口
- `init_db.py`: 数据库初始化脚本

### 2. 数据库设置
MongoDB数据库已配置完成，包括：

#### 集合
1. `tweets` 集合：存储推文数据
   - 字段：id, username, text, created_at, metrics, fetched_at
   - 索引：
     * id (唯一索引)
     * username
     * created_at

2. `users` 集合：存储用户信息
   - 字段：id, username, added_at, is_active
   - 索引：
     * username (唯一索引)
     * id

## 待完成工作

### 1. Twitter API配置
需要在Twitter开发者平台申请以下API密钥：
- Bearer Token
- API Key
- API Secret
- Access Token
- Access Token Secret

### 2. 环境变量设置
完成API申请后，需要在`.env`文件中配置以下环境变量：
```
TWITTER_BEARER_TOKEN=your_bearer_token_here
TWITTER_API_KEY=your_api_key_here
TWITTER_API_SECRET=your_api_secret_here
TWITTER_ACCESS_TOKEN=your_access_token_here
TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret_here
```

## 使用方法

### 1. 添加跟踪用户
```bash
python -m twitter_tracker.cli add <username>
```

### 2. 获取用户推文
```bash
python -m twitter_tracker.cli fetch <username> --max-results 100
```

### 3. 查看用户数据分析
```bash
python -m twitter_tracker.cli analytics <username>
```

### 4. 查看所有跟踪的用户
```bash
python -m twitter_tracker.cli list_users
```

### 5. 停止跟踪用户
```bash
python -m twitter_tracker.cli remove <username>
```

## 数据分析功能
- 推文总数统计
- 平均点赞数
- 平均转发数
- 平均回复数
- 最新和最早推文时间记录

## 注意事项
1. 使用前确保MongoDB服务正在运行
2. 需要有效的Twitter API密钥
3. 所有数据都会存储在本地MongoDB数据库中
4. 支持增量更新，不会重复存储相同的推文

## 后续优化方向
1. 添加数据可视化功能
   - 使用Dash或Streamlit创建Web界面
   - 显示推文趋势图表
   - 互动式数据探索界面
   - 实时数据更新显示

2. 实现自动定时获取推文
   - 添加定时任务调度
   - 配置灵活的更新频率
   - 失败重试机制
   - 异常通知机制

3. 添加关键词过滤功能
   - 支持多关键词组合
   - 正则表达式匹配
   - 关键词权重设置
   - 自动关键词提取

4. 添加情感分析
   - 集成自然语言处理模型
   - 多语言支持
   - 情感趋势分析
   - 关键词情感关联分析

5. 数据导出功能
   - 支持多种格式（CSV, JSON, Excel）
   - 自定义导出字段
   - 数据清洗选项
   - 批量导出功能

6. 高级分析功能
   - 用户互动网络分析
   - 话题传播路径追踪
   - 影响力指标计算
   - 用户画像分析

7. 系统优化
   - 数据库性能优化
   - 内存使用优化
   - 并发请求处理
   - 错误处理完善

8. 监控告警功能
   - 关键词触发告警
   - 异常活动监测
   - 自定义告警规则
   - 多渠道通知（邮件、Slack等）

9. API集成扩展
   - 支持多平台数据源（LinkedIn, Facebook等）
   - API限流管理
   - 代理支持
   - 身份认证管理

10. 用户管理功能
    - 多用户支持
    - 权限管理
    - 用户操作日志
    - 配置管理界面

11. 数据备份与恢复
    - 自动备份机制
    - 增量备份支持
    - 快速恢复功能
    - 历史版本管理

12. 报告生成功能
    - 自动生成分析报告
    - 自定义报告模板
    - 定时发送功能
    - 多种报告格式支持

## 开发路线图

### 第一阶段 - 基础功能完善
- [x] 基础框架搭建
- [x] MongoDB数据库配置
- [ ] Twitter API集成
- [ ] 基础命令行工具完善

### 第二阶段 - 核心功能增强
- [ ] 数据可视化界面开发
- [ ] 自动定时获取功能
- [ ] 关键词过滤系统
- [ ] 基础情感分析

### 第三阶段 - 高级功能开发
- [ ] 高级数据分析功能
- [ ] 监控告警系统
- [ ] 多平台支持
- [ ] 用户管理系统

### 第四阶段 - 系统优化与扩展
- [ ] 性能优化
- [ ] 安全性增强
- [ ] 可扩展性改进
- [ ] 文档完善

## 技术栈
- 后端：Python, MongoDB
- 数据分析：Pandas, NumPy
- 可视化：Dash/Streamlit
- NLP：Transformers/NLTK
- 部署：Docker
