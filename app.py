from PIL.ImageChops import offset
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
import asyncio
import sqlite3
from typing import List, Dict, Any

# 导入你提供的Update和dataBase类
from table import dataBase
from update import Update
from DatabaseManager import DatabaseManager# 请替换为实际模块名

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 用于flash消息

# 初始化数据库管理器
db_manager = DatabaseManager()


def generate_page_range(current_page, total_pages, max_display=7):
    """生成智能分页范围"""
    if total_pages <= max_display:
        return list(range(1, total_pages + 1))
    elif current_page <= max_display - 2:
        return list(range(1, max_display)) + ['...', total_pages]
    elif current_page >= total_pages - (max_display - 3):
        return [1, '...'] + list(range(total_pages - (max_display - 2), total_pages + 1))
    else:
        start = current_page - (max_display - 4) // 2
        end = current_page + (max_display - 4) // 2
        return [1, '...'] + list(range(start, end + 1)) + ['...', total_pages]


@app.route('/')
def index():
    """主页 - 显示所有表格"""
    tables = db_manager.get_all_tables()
    table_info = []

    for table in tables:
        count = db_manager.get_video_count(table)
        last_update = db_manager.get_last_update_time(table)
        can_update = db_manager.can_update(table)
        table_info.append({
            'name': table,
            'count': count,
            'last_update': last_update,
            'can_update': can_update
        })

    return render_template('index.html', table_info=table_info)


@app.route('/table/<table_name>')
def show_table(table_name):
    """显示指定表的数据"""
    page = request.args.get('page', 1, type=int)
    search_term = request.args.get('search', '')
    sort_by = request.args.get('sort_by', 'score')
    order = request.args.get('order', 'DESC')

    per_page = 20

    # 获取数据
    if search_term:
        all_data = db_manager.search_videos(table_name, search_term, sort_by, order)
        data = all_data[offset:offset + per_page]
        total_count = len(all_data)
    else:
        result = db_manager.get_table_data_paginated(table_name, page, per_page, sort_by, order)
        data = result["data"]
        total_count = result["total_count"]

    total_pages = (total_count + per_page - 1) // per_page
    page_range = generate_page_range(page, total_pages)
    last_update = db_manager.get_last_update_time(table_name)
    can_update = db_manager.can_update(table_name)

    return render_template('table.html',
                           table_name=table_name,
                           data=data,
                           page=page,
                           total_pages=total_pages,
                           page_range=page_range,
                           search_term=search_term,
                           sort_by=sort_by,
                           order=order,
                           total_count=total_count,
                           last_update=last_update,
                           can_update=can_update)


@app.route('/update/<table_name>', methods=['POST'])
def update_table(table_name):
    """更新指定表的数据"""
    # 检查是否可以更新
    if not db_manager.can_update(table_name):
        flash(f'表 "{table_name}" 距离上次更新不足3小时，请稍后再试。', 'warning')
        return redirect(url_for('show_table', table_name=table_name))

    try:
        # 异步执行更新
        asyncio.run(update_table_async(table_name))
        db_manager.log_update(table_name)
        flash(f'表 "{table_name}" 更新成功！', 'success')
    except Exception as e:
        flash(f'更新表 "{table_name}" 时出错: {str(e)}', 'danger')

    return redirect(url_for('show_table', table_name=table_name))


async def update_table_async(table_name):
    """异步更新表数据"""
    # 使用表名作为搜索主题
    updater = Update(table_name)
    database = dataBase("database.db")

    # 确保表存在
    await database.createTable(table_name)

    # 获取视频数据
    videos = await updater.getVideos()

    # 更新数据库
    if videos:
        await database.updateTable(table_name, videos)
        print(f"成功更新表 {table_name}，添加了 {len(videos)} 条记录")
    else:
        print(f"表 {table_name} 没有获取到新数据")


@app.route('/api/table/<table_name>')
def api_table_data(table_name):
    """API接口 - 返回JSON格式的表数据"""
    limit = request.args.get('limit', 100, type=int)
    data = db_manager.get_table_data_safe(table_name, limit)
    return jsonify({
        'table_name': table_name,
        'data': data,
        'count': len(data)
    })


@app.route('/stats')
def stats():
    """统计页面"""
    tables = db_manager.get_all_tables()
    stats_data = []

    for table in tables:
        count = db_manager.get_video_count(table)
        last_update = db_manager.get_last_update_time(table)
        can_update = db_manager.can_update(table)
        stats_data.append({
            'table_name': table,
            'video_count': count,
            'last_update': last_update,
            'can_update': can_update
        })

    return render_template('stats.html', stats_data=stats_data)


def create_table( name ):
    """创建新表"""
    table_name = name
    if table_name:
        try:
            # 创建空表
            database = dataBase("database.db")
            asyncio.run(database.createTable(table_name))
        except Exception as e:
            print(f'创建表 "{table_name}" 时出错: {str(e)}')




if __name__ == '__main__':
    # 什么主题
    names = ["哈基米音乐", "OI"]
    for name in names:
        create_table( name )
    # 启动Flask应用
    print("启动Flask应用...")
    app.run(debug=True, host='0.0.0.0', port=5001)