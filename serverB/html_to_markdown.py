"""
HTML 表格转 Markdown 模块
"""
from html.parser import HTMLParser
from typing import List, Optional


class HTMLTableParser(HTMLParser):
    """HTML 表格解析器"""
    
    def __init__(self):
        super().__init__()
        self.tables = []
        self.current_table = None
        self.current_row = None
        self.current_cell = None
        self.in_table = False
        self.in_row = False
        self.in_cell = False
    
    def handle_starttag(self, tag, attrs):
        if tag == 'table':
            self.in_table = True
            self.current_table = []
        elif tag == 'tr' and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag in ['td', 'th'] and self.in_row:
            self.in_cell = True
            self.current_cell = ''
    
    def handle_endtag(self, tag):
        if tag == 'table':
            self.in_table = False
            if self.current_table:
                self.tables.append(self.current_table)
            self.current_table = None
        elif tag == 'tr' and self.in_row:
            self.in_row = False
            if self.current_row is not None:
                self.current_table.append(self.current_row)
            self.current_row = None
        elif tag in ['td', 'th'] and self.in_cell:
            self.in_cell = False
            if self.current_cell is not None:
                self.current_row.append(self.current_cell.strip())
            self.current_cell = None
    
    def handle_data(self, data):
        if self.in_cell and self.current_cell is not None:
            self.current_cell += data


def html_table_to_markdown(html: str) -> Optional[str]:
    """
    将 HTML 表格转换为 Markdown 格式
    
    参数:
        html: HTML 表格字符串
    
    返回:
        Markdown 格式的表格，如果解析失败返回 None
    """
    if not html or '<table' not in html.lower():
        return None
    
    try:
        parser = HTMLTableParser()
        parser.feed(html)
        
        if not parser.tables or not parser.tables[0]:
            return None
        
        # 取第一个表格
        table_data = parser.tables[0]
        
        if not table_data:
            return None
        
        # 构造 Markdown 表格
        markdown_lines = []
        
        # 第一行（表头）
        if len(table_data) > 0:
            header = table_data[0]
            markdown_lines.append('| ' + ' | '.join(header) + ' |')
            
            # 分隔线
            markdown_lines.append('| ' + ' | '.join(['---'] * len(header)) + ' |')
        
        # 数据行
        for row in table_data[1:]:
            # 补齐列数（以表头为准）
            if len(table_data) > 0:
                header_len = len(table_data[0])
                while len(row) < header_len:
                    row.append('')
            
            markdown_lines.append('| ' + ' | '.join(row) + ' |')
        
        return '\n'.join(markdown_lines)
    
    except Exception as e:
        # 解析失败，返回 None
        print(f"HTML to Markdown conversion failed: {e}")
        return None


def test_html_to_markdown():
    """测试函数"""
    html = """
    <table>
        <tr><th>Name</th><th>Age</th><th>City</th></tr>
        <tr><td>Alice</td><td>30</td><td>NYC</td></tr>
        <tr><td>Bob</td><td>25</td><td>LA</td></tr>
    </table>
    """
    
    result = html_table_to_markdown(html)
    print(result)


if __name__ == '__main__':
    test_html_to_markdown()