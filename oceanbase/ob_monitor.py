#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OceanBase 数据监控程序

实时展示 OceanBase 数据库中的数据情况，包括：
- 数据库连接状态
- 表数据统计
- 数据质量统计
- 实时数据查询
"""

import subprocess
import sys
import json
from datetime import datetime
from typing import List, Dict, Any, Optional

# ==================== 配置 ====================

OCEANBASE_HOST = "localhost"
OCEANBASE_PORT = "2881"
OCEANBASE_USER = "root"
OCEANBASE_PASSWORD = "Audaque@123"
OCEANBASE_DATABASE = "kafka_quality_check"

# ==================== 数据库执行函数 ====================

def execute_ob_sql(sql: str) -> str:
    """执行 OceanBase SQL 命令"""
    cmd = [
        "docker", "exec", "oceanbase",
        "obclient",
        "-h", OCEANBASE_HOST,
        "-P", OCEANBASE_PORT,
        "-u", OCEANBASE_USER + "@" + "test",
        f"-p{OCEANBASE_PASSWORD}",
        "-D", OCEANBASE_DATABASE,
        "-e", sql
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            return f"Error: {result.stderr}"
        return result.stdout
    except subprocess.TimeoutExpired:
        return "Error: SQL execution timeout"
    except Exception as e:
        return f"Error: {str(e)}"


def check_connection() -> bool:
    """检查数据库连接"""
    result = execute_ob_sql("SELECT 1;")
    return "1" in result and "Error" not in result


# ==================== 数据展示函数 ====================

def print_header(title: str):
    """打印标题头"""
    width = 70
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width)


def print_table(headers: List[str], rows: List[List[str]], max_width: int = 20):
    """打印表格"""
    if not rows:
        print("  (无数据)")
        return

    # 计算列宽
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            cell_str = str(cell)[:max_width]
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell_str))

    # 打印表头
    header_line = "  | " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"
    separator = "  +" + "+".join("-" * (w + 2) for w in col_widths) + "+"

    print(separator)
    print(header_line)
    print(separator)

    # 打印数据行
    for row in rows:
        row_line = "  | " + " | ".join(str(cell)[:max_width].ljust(col_widths[i]) for i, cell in enumerate(row)) + " |"
        print(row_line)

    print(separator)


def show_database_info():
    """显示数据库基本信息"""
    print_header("OceanBase 数据库信息")

    # 数据库版本
    sql = "SELECT VERSION();"
    result = execute_ob_sql(sql)
    print(f"  版本：{result.strip().split(chr(10))[-2] if chr(10) in result else result.strip()}")

    # 数据库名称
    sql = "SELECT DATABASE();"
    result = execute_ob_sql(sql)
    lines = [l for l in result.strip().split(chr(10)) if l and 'DATABASE()' not in l and '+-' not in l]
    if lines:
        print(f"  当前数据库：{lines[-1].strip()}")

    # 当前时间
    sql = "SELECT NOW();"
    result = execute_ob_sql(sql)
    lines = [l for l in result.strip().split(chr(10)) if l and 'NOW()' not in l and '+-' not in l]
    if lines:
        print(f"  服务器时间：{lines[-1].strip()}")


def show_table_list():
    """显示表列表"""
    print_header("数据表列表")

    sql = "SHOW TABLES;"
    result = execute_ob_sql(sql)

    lines = [l for l in result.strip().split(chr(10)) if l and '+-' not in l and 'Tables_in' not in l]

    if lines:
        print("  当前数据库中的表:")
        for table in lines:
            print(f"    - {table.strip()}")
    else:
        print("  (无表)")


def show_invalid_data_stats():
    """显示问题数据统计"""
    print_header("问题数据 (invalid_data) 统计")

    # 总数统计
    sql = "SELECT COUNT(*) as total FROM invalid_data;"
    result = execute_ob_sql(sql)
    lines = [l for l in result.strip().split(chr(10)) if l.strip() and '+-' not in l and 'total' not in l]
    total = lines[-1].strip() if lines else "0"
    print(f"  问题数据总数：{total}")

    # 按操作类型统计
    sql = "SELECT opcode as '操作类型', COUNT(*) as '数量' FROM invalid_data GROUP BY opcode ORDER BY COUNT(*) DESC;"
    result = execute_ob_sql(sql)
    print("\n  按操作类型统计:")
    parse_and_print_table(result, prefix="    ")

    # 按表名统计
    sql = "SELECT table_name as '表名', COUNT(*) as '数量' FROM invalid_data GROUP BY table_name ORDER BY COUNT(*) DESC LIMIT 5;"
    result = execute_ob_sql(sql)
    print("\n  按表名统计 (Top 5):")
    parse_and_print_table(result, prefix="    ")

    # 错误类型统计
    sql = """
    SELECT
        '性别错误' as '错误类型',
        SUM(CASE WHEN error_details LIKE '%性别%' THEN 1 ELSE 0 END) as '数量'
    FROM invalid_data
    UNION ALL
    SELECT '年龄错误', SUM(CASE WHEN error_details LIKE '%年龄%' THEN 1 ELSE 0 END) FROM invalid_data
    UNION ALL
    SELECT '电话错误', SUM(CASE WHEN error_details LIKE '%电话%' THEN 1 ELSE 0 END) FROM invalid_data
    UNION ALL
    SELECT '血型错误', SUM(CASE WHEN error_details LIKE '%血型%' THEN 1 ELSE 0 END) FROM invalid_data
    UNION ALL
    SELECT '信用分错误', SUM(CASE WHEN error_details LIKE '%信用分%' THEN 1 ELSE 0 END) FROM invalid_data
    UNION ALL
    SELECT '住房面积错误', SUM(CASE WHEN error_details LIKE '%住房面积%' THEN 1 ELSE 0 END) FROM invalid_data
    ORDER BY '数量' DESC;
    """
    result = execute_ob_sql(sql)
    print("\n  按错误类型统计:")
    parse_and_print_table(result, prefix="    ")


def show_quality_stats():
    """显示质量统计"""
    print_header("数据质量统计 (quality_stats)")

    sql = """
    SELECT
        stat_date as '日期',
        table_name as '表名',
        total_count as '总数',
        invalid_count as '问题数',
        valid_count as '正常数',
        ROUND(invalid_rate, 2) as '问题率%'
    FROM quality_stats
    ORDER BY stat_date DESC, stat_hour DESC
    LIMIT 10;
    """
    result = execute_ob_sql(sql)
    parse_and_print_table(result)


def show_recent_invalid_data(limit: int = 10):
    """显示最近的问题数据"""
    print_header(f"最近的问题数据 (Top {limit})")

    sql = f"""
    SELECT
        id,
        record_key as '记录键',
        table_name as '表名',
        opcode as '操作',
        personid as '人员 ID',
        name as '姓名',
        failure_summary as '失败原因'
    FROM invalid_data
    ORDER BY log_timestamp DESC
    LIMIT {limit};
    """
    result = execute_ob_sql(sql)
    parse_and_print_table(result)


def parse_and_print_table(result: str, prefix: str = "  "):
    """解析并打印表格结果"""
    lines = result.strip().split(chr(10))
    if len(lines) < 2:
        print(f"{prefix}(无数据)")
        return

    # 过滤掉表格边框线
    data_lines = [l for l in lines if '+-' not in l and l.strip()]

    if len(data_lines) < 2:
        print(f"{prefix}(无数据)")
        return

    headers = data_lines[0].split(chr(9))  # tab 分隔
    rows = [line.split(chr(9)) for line in data_lines[1:]]

    # 简化打印
    if headers:
        print(f"{prefix} | ".join(str(h).strip() for h in headers))
        print(f"{prefix}" + "-" * 50)
        for row in rows:
            print(f"{prefix} | ".join(str(cell).strip() for cell in row))


def show_dashboard():
    """显示完整监控面板"""
    clear_screen()

    print_header("OceanBase 数据监控面板")
    print(f"  刷新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  连接：{OCEANBASE_HOST}:{OCEANBASE_PORT}/{OCEANBASE_DATABASE}")

    # 检查连接
    if not check_connection():
        print("\n  [错误] 无法连接到 OceanBase 数据库!")
        print(f"  请检查：docker ps | grep oceanbase")
        return False

    print("\n  [✓] 数据库连接正常")

    show_database_info()
    show_table_list()
    show_invalid_data_stats()
    show_quality_stats()
    show_recent_invalid_data(5)

    return True


def clear_screen():
    """清屏"""
    subprocess.run(["clear"] if sys.platform != "win32" else ["cls"])


def show_help():
    """显示帮助信息"""
    print_header("OceanBase 数据监控程序 - 帮助")
    print("""
  命令说明:

    1. dashboard / d    - 显示完整监控面板
    2. stats / s        - 显示数据统计
    3. recent / r [n]   - 显示最近 n 条问题数据 (默认 10)
    4. tables / t       - 显示表列表
    5. query / q        - 执行自定义 SQL 查询
    6. refresh / f      - 刷新当前视图
    7. help / h         - 显示帮助信息
    8. quit / exit / q  - 退出程序

  示例:

    > dashboard         # 显示完整面板
    > recent 20         # 显示最近 20 条问题数据
    > query SELECT COUNT(*) FROM invalid_data;
    """)


def interactive_mode():
    """交互模式"""
    print_header("OceanBase 数据监控程序")
    print(f"  连接：{OCEANBASE_HOST}:{OCEANBASE_PORT}/{OCEANBASE_DATABASE}")
    print("  输入 'help' 查看命令列表，'quit' 退出")
    print()

    while True:
        try:
            cmd = input("ob-monitor> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  再见!")
            break

        if not cmd:
            continue

        parts = cmd.split()
        command = parts[0]
        args = parts[1:] if len(parts) > 1 else []

        if command in ["quit", "exit", "q"]:
            print("  再见!")
            break
        elif command in ["help", "h", "?"]:
            show_help()
        elif command in ["dashboard", "d"]:
            show_dashboard()
        elif command in ["stats", "s"]:
            print_header("数据统计")
            show_invalid_data_stats()
            show_quality_stats()
        elif command in ["tables", "t"]:
            print_header("数据表")
            show_table_list()
        elif command in ["recent", "r"]:
            limit = int(args[0]) if args else 10
            show_recent_invalid_data(limit)
        elif command in ["query", "q"]:
            if args:
                sql = " ".join(args)
                result = execute_ob_sql(sql)
                parse_and_print_table(result)
            else:
                print("  用法：query <SQL 语句>")
        elif command in ["refresh", "f"]:
            clear_screen()
            show_dashboard()
        else:
            print(f"  未知命令：{command}，输入 'help' 查看帮助")


def batch_mode():
    """批处理模式 - 一次性输出"""
    if show_dashboard():
        print("\n  数据导出完成")
    else:
        print("\n  数据导出失败")


# ==================== 主程序 ====================

def main():
    """主程序入口"""
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg in ["-h", "--help"]:
            show_help()
        elif arg in ["-b", "--batch"]:
            batch_mode()
        elif arg in ["-i", "--interactive"]:
            interactive_mode()
        else:
            print(f"未知参数：{arg}")
            print("使用 -h 或 --help 查看帮助")
    else:
        # 默认交互模式
        interactive_mode()


if __name__ == "__main__":
    main()
