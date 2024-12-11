import click
from .twitter_api import TwitterTracker
import json

@click.group()
def cli():
    """Twitter追踪工具 - 跟踪并分析Twitter用户的推文"""
    pass

@cli.command()
@click.argument('username')
def add(username):
    """添加要跟踪的Twitter用户"""
    tracker = TwitterTracker()
    if tracker.add_user(username):
        click.echo(f"成功添加用户: {username}")
    else:
        click.echo(f"添加用户失败: {username}")

@cli.command()
@click.argument('username')
@click.option('--max-results', default=100, help='获取的最大推文数量')
def fetch(username, max_results):
    """获取用户的最新推文"""
    tracker = TwitterTracker()
    tweets = tracker.fetch_user_tweets(username, max_results)
    if tweets:
        click.echo(f"成功获取 {len(tweets)} 条推文")
        for tweet in tweets:
            click.echo(f"[{tweet['created_at']}] {tweet['text'][:100]}...")
    else:
        click.echo("未获取到推文")

@cli.command()
@click.argument('username')
def analytics(username):
    """获取用户的推文分析数据"""
    tracker = TwitterTracker()
    stats = tracker.get_user_analytics(username)
    if stats:
        click.echo(json.dumps(stats, indent=2, default=str))
    else:
        click.echo("未找到分析数据")

@cli.command()
def list_users():
    """列出所有正在跟踪的用户"""
    tracker = TwitterTracker()
    users = tracker.get_all_tracked_users()
    if users:
        click.echo("正在跟踪的用户:")
        for user in users:
            click.echo(f"- {user}")
    else:
        click.echo("当前没有跟踪任何用户")

@cli.command()
@click.argument('username')
def remove(username):
    """停止跟踪某个用户"""
    tracker = TwitterTracker()
    if tracker.remove_user(username):
        click.echo(f"已停止跟踪用户: {username}")
    else:
        click.echo(f"停止跟踪失败: {username}")

if __name__ == '__main__':
    cli()
