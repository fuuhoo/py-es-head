import nicegui.ui as ui
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError
import json
from datetime import datetime
import pandas as pd
import io
from elasticsearch.helpers import scan
import asyncio
import threading
from nicegui import ui, app as ng_app

class ElasticsearchHead:
    def __init__(self):
        self.es = None
        self.current_index = None
        self.fields = []
        self.index_stats = {}  # 存储索引统计信息
        self.query_conditions_list = []  # 存储查询条件
        self.current_query_result = None  # 存储当前查询结果
        self.query=""  # 存储当前查询语句
        self.main_loop=asyncio.get_event_loop()
        self.export_status_label=None
        self.notification_container = ui.column().classes('fixed top-0 right-0 m-4 z-50')

        # # 设置页面样式
        # self.setup_styles()
        # # 创建主界面
        # self.setup_ui()
    def update_export_status(self,status):
        self.export_status_label.text=f'导出状态:{status}'
    def setup_styles(self):
        """设置清晰明亮的样式"""
        ui.add_head_html('''
            <style>
                :root {
                    --primary-color: #2563eb;
                    --secondary-color: #1d4ed8;
                    --accent-color: #10b981;
                    --success-color: #059669;
                    --warning-color: #d97706;
                    --danger-color: #dc2626;
                    --bg-light: #f8fafc;
                    --bg-white: #ffffff;
                    --bg-card: #ffffff;
                    --bg-header: #2563eb;
                    --text-primary: #1f2937;
                    --text-secondary: #6b7280;
                    --text-light: #9ca3af;
                    --border-color: #e5e7eb;
                    --border-light: #f3f4f6;
                    --shadow-color: rgba(37, 99, 235, 0.1);
                    --shadow-light: rgba(0, 0, 0, 0.05);
                }
                
                body {
                    background-color: var(--bg-light);
                    color: var(--text-primary);
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    margin: 0;
                    padding: 0;
                    width: 100%;
                }
                
                .full-width {
                    width: 100% !important;
                    max-width: 100% !important;
                }
                
                .query-builder-container {
                    width: 100% !important;
                    max-width: 100% !important;
                }
                
                .custom-primary { color: var(--primary-color); }
                .custom-bg-primary { background-color: var(--primary-color); }
                .custom-border { 
                    border: 1px solid var(--border-color); 
                    border-radius: 0.15rem; 
                    background-color: var(--bg-card);
                    box-shadow: 0 1px 3px var(--shadow-light);
                }
                
                .index-card { 
                    transition: all 0.3s ease; 
                    cursor: pointer; 
                    min-width: 220px; 
                    max-width: 260px;
                    flex-shrink: 0;
                    background: var(--bg-card);
                    border: 2px solid var(--border-light);
                    border-radius: 12px;
                    padding: 6px;
                    position: relative;
                    box-shadow: 0 2px 8px var(--shadow-light);
                }
                
                .index-card:hover { 
                    border-color: var(--primary-color);
                    transform: translateY(-2px); 
                    box-shadow: 0 8px 25px var(--shadow-color);
                }
                
                .stats-badge { 
                    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
                    color: white;
                    font-weight: 600;
                    border-radius: 0.5rem;
                    padding: 0.25rem 0.25rem;
                    font-size: 0.25rem;
                }
                
                .selected-index { 
                    border: 2px solid var(--primary-color);
                    background: linear-gradient(135deg, #eff6ff, #dbeafe);
                    transform: translateY(-1px);
                    box-shadow: 0 8px 25px var(--shadow-color);
                }
                
                .indices-container {
                    display: flex;
                    gap: 0.5rem;
                    overflow-x: auto;
                    padding: 0.5rem 0;
                    scroll-behavior: smooth;
                    max-height: 180px;
                }
                
                .indices-container::-webkit-scrollbar {
                    height: 6px;
                }
                
                .indices-container::-webkit-scrollbar-track {
                    background: var(--border-light);
                    border-radius: 3px;
                }
                
                .indices-container::-webkit-scrollbar-thumb {
                    background: var(--primary-color);
                    border-radius: 3px;
                }
                
                .indices-container::-webkit-scrollbar-thumb:hover {
                    background: var(--secondary-color);
                }
                
                .index-stats-grid {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 0.5rem;
                    margin-top: 0.25rem;
                }
                
                .stat-item {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 0.25rem 0.25rem;
                    background: var(--bg-light);
                    border-radius: 0.5rem;
                    font-size: 0.25rem;
                    color: var(--text-secondary);
                }
                
                .doc-count-highlight {
                    background: linear-gradient(135deg, var(--accent-color), var(--success-color));
                    color: white;
                    padding: 0.5rem 0.25rem;
                    border-radius: 0.25rem;
                    font-weight: 600;
                    text-align: center;
                    margin-bottom: 0.25rem;
                    font-size: 0.875rem;
                    box-shadow: 0 2px 8px rgba(16, 185, 129, 0.3);
                }
                
                .clear-button {
                    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
                    color: white;
                    border: none;
                    border-radius: 0.25rem;
                    padding: 0.25rem 0.5rem;
                    font-weight: 600;
                    transition: all 0.3s ease;
                    cursor: pointer;
                    box-shadow: 0 2px 8px var(--shadow-color);
                }
                
                .clear-button:hover {
                    transform: translateY(-1px);
                    box-shadow: 0 4px 15px var(--shadow-color);
                }
                
                .tech-input {
                    background-color: var(--bg-white);
                    border: 2px solid var(--border-color);
                    color: var(--text-primary);
                    border-radius: 0.25rem;
                    padding: 0.25rem 0.25rem;
                    transition: all 0.3s ease;
                }
                
                .tech-input:focus {
                    border-color: var(--primary-color);
                    box-shadow: 0 0 0 3px var(--shadow-color);
                    outline: none;
                }
                
                .connection-status {
                    padding: 0.5rem 0.5rem;
                    border-radius: 0.25rem;
                    font-weight: 600;
                    font-size: 0.875rem;
                }
                
                .status-connected {
                    background: linear-gradient(135deg, var(--accent-color), var(--success-color));
                    color: white;
                }
                
                .status-disconnected {
                    background: linear-gradient(135deg, var(--danger-color), #b91c1c);
                    color: white;
                }
                
                .clear-header {
                    background: linear-gradient(135deg, var(--primary-color), var(--secondary-color));
                    color: white;
                }
                
                .index-type-badge {
                    font-size: 0.25rem;
                    padding: 0.25rem 0.25rem;
                    border-radius: 0.5rem;
                    font-weight: 600;
                }
                
                .system-index {
                    background: linear-gradient(135deg, #6b7280, #4b5563);
                    color: white;
                }
                
                .user-index {
                    background: linear-gradient(135deg, var(--accent-color), var(--success-color));
                    color: white;
                }
                
                .action-button {
                    background: var(--primary-color);
                    color: white;
                    border: none;
                    border-radius: 0.3rem;
                    padding: 0.5rem;
                    font-size: 0.75rem;
                    cursor: pointer;
                    transition: all 0.3s ease;
                }
                
                .action-button:hover {
                    background: var(--secondary-color);
                    transform: scale(1.05);
                }
                
                .delete-button {
                    background: var(--danger-color);
                    color: white;
                    border: none;
                    border-radius: 0.5rem;
                    padding: 0.25rem 0.25rem;
                    font-size: 0.25rem;
                    cursor: pointer;
                    transition: all 0.3s ease;
                }
                
                .delete-button:hover {
                    background: #b91c1c;
                    transform: scale(1.05);
                }
                
                .tab-style {
                    background: var(--bg-card);
                    border: 1px solid var(--border-color);
                    border-radius: 0.25rem;
                    color: var(--text-secondary);
                }
                
                .tab-style.active {
                    background: var(--primary-color);
                    color: white;
                    border-color: var(--primary-color);
                }
                
                .export-button {
                    background: linear-gradient(135deg, #059669, #10b981);
                    color: white;
                    border: none;
                    border-radius: 0.25rem;
                    padding: 0.25rem 0.25rem;
                    font-weight: 600;
                    transition: all 0.3s ease;
                    cursor: pointer;
                    box-shadow: 0 2px 8px rgba(5, 150, 105, 0.3);
                }
                
                .export-button:hover {
                    transform: translateY(-1px);
                    box-shadow: 0 4px 15px rgba(5, 150, 105, 0.4);
                }

                .q-field__control{
                    height: 100%!important;
                }
                .q-field__label{
                    top: 10px!important;
                }
                .q-field__native {
                    line-height: 15px!important;
                }

                .range-input   .q-field__native {
                    line-height: 35px!important;
                }

            </style>
        ''')

    async def show_notify(self, msg, type=""):
            """在正确的上下文中显示通知"""
            # 确保在容器上下文中创建通知
            with self.notification_container:
                # 使用Quasar通知组件替代ui.notify
                ui.notify(msg, type=type, position='top-right')
    def custom_notify(self, text, type=""):
        with self.notification_container:
            # 使用Quasar通知组件替代ui.notify
            ui.notify(text, type="", position='top-right')      
    def async_notify(self, text, type=""):
        """异步显示通知"""
        if self.main_loop and not self.main_loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                self.show_notify(text, type=""),
                loop=self.main_loop
        )
    # def async_notify(self, text, type):
    #     print("事件循环", self.main_loop)
    #     """异步显示通知"""
    #     # 添加调试信息
    #     print(f"通知内容: {text}, 类型: {type}")
        
    #     # 检查事件循环状态
    #     if self.main_loop is None:
    #         print("错误: 事件循环为None")
    #         return
            
    #     if self.main_loop.is_closed():
    #         print("错误: 事件循环已关闭")
    #         return
        
    #     try:
    #         # 添加异常捕获
    #         future = asyncio.run_coroutine_threadsafe(
    #             self.show_notify(text, type),
    #             loop=self.main_loop
    #         )
    #         # 等待结果（可选，用于调试）
    #         future.result(timeout=5)  # 等待5秒
    #         print("通知已调度")
    #     except Exception as e:
    #         print(f"调度通知时出错: {e}")

    # async def show_notify(self, msg, type):
    #     """显示通知"""
    #     try:
    #         print(f"准备显示通知: {msg}")
    #         ui.notify(msg, type=type)
    #         print("通知显示调用完成")
    #     except Exception as e:
    #         print(f"显示通知时出错: {e}")
  
    # def async_notify(self, text, type):
    #     print("事件循环",self.main_loop)
    #     """异步显示通知"""
    #     asyncio.run_coroutine_threadsafe(
    #         self.show_notify(text, type),
    #         loop=self.main_loop
    #     )

    # async def show_notify(self, msg, type):
    #     """显示通知"""
    #     ui.notify(msg, type=type)

    def setup_ui(self):
        """设置清晰的用户界面"""
        # 设置页面宽度为100%
        ui.query('body').style('width: 100% !important; max-width: 100% !important;')
        
        # 标题和连接面板 - 清晰明亮样式
        with ui.header().classes('clear-header shadow-lg w-full'):
            with ui.row().classes('w-full items-center justify-between'):
                ui.label('🔍 py-es-head').classes('text-h4 text-weight-bold')
                with ui.row().classes('items-center gap-3'):
                    self.host_input = ui.input('ES 地址', value='172.17.4.24:9200').classes('w-40 tech-input')
                    self.username_input = ui.input('用户名').classes('w-30 tech-input')
                    self.password_input = ui.input('密码', password=True).classes('w-30 tech-input')
                    ui.button('🔗 连接', on_click=self.connect_es).classes('clear-button')
                    self.connection_status = ui.label('● 未连接').classes('connection-status status-disconnected')
        
        # 主要内容区域
        with ui.column().classes('w-full p-1 gap-6'):
            # 横向索引列表区域 - 清晰设计
            with ui.card().classes('w-full custom-border p-1'):
                with ui.row().classes('w-full items-center justify-between mb-4'):
                    ui.label('📊 索引列表').classes('text-h6 font-bold custom-primary')
                    
                    with ui.row().classes('items-center gap-3'):
                        # 索引搜索
                        self.index_search = ui.input(placeholder='🔍 搜索索引...', on_change=self.filter_indices).classes('w-64 tech-input')
                        ui.button('🔄 刷新', on_click=self.refresh_indices).classes('clear-button')
                
                # 横向滚动的索引容器
                self.index_container = ui.row().classes('indices-container w-full')
            
            # 查询构建器和结果区域 - 修改为全宽度布局
            with ui.column().classes('w-full gap-4 query-builder-container'):
                # 当前索引信息
                with ui.card().classes('w-full custom-border p-4'):
                    with ui.row().classes('w-full items-center justify-between'):
                        self.current_index_label = ui.label('📁 未选择索引').classes('text-h6 font-bold custom-primary')
                        self.index_doc_count = ui.label('').classes('text-sm stats-badge')
                

            #  查询构建器
            with ui.column().classes('w-full gap-4'):
                with ui.card().classes('w-full custom-border p-4'):
                    ui.label('🔧 查询构建器').classes('text-h6 font-bold custom-primary mb-4')
                    
                    # 查询条件输入行
                    with ui.row().classes('w-full items-end gap-3'):

                        self.query_field = ui.select(
                            label='字段',
                            options=[],
                            with_input=True
                        ).classes('flex-1 tech-input')
                        
                        self.query_type = ui.select(
                            label='查询类型',
                            options=['match', 'term', 'range', 'wildcard', 'exists'],
                            value='match'
                        ).classes('flex-1 tech-input')
                        # 添加查询类型改变事件
                        self.query_type.on('update:model-value', self.on_query_type_change)
                        
                        # 动态显示不同的值输入控件
                        self.query_value_container = ui.column().classes('flex-1 tech-input')
                        self.setup_query_value_input()
                        
                        ui.button('➕ 添加条件', on_click=self.add_query_condition).classes('clear-button')
                    
                    # 查询条件列表
                    self.query_conditions = ui.aggrid({
                        'columnDefs': [
                            {'headerName': '字段', 'field': 'field', 'flex': 1},
                            {'headerName': '类型', 'field': 'type', 'flex': 1},
                            {'headerName': '值', 'field': 'value', 'flex': 2},
                            {
                                'headerName': '操作', 
                                'field': 'action', 
                                'flex': 1,
                                'cellRenderer': 'ButtonRenderer',
                                'cellRendererParams': {
                                    'label': '删除',
                                    'className': 'delete-button'
                                }
                            }
                        ],
                        'rowData': [],
                        'rowHeight': 40,
                        'theme': 'ag-theme-alpine'
                    }).classes('h-48 w-full mt-4')
                    
                    # 添加单元格点击事件
                    self.query_conditions.on('cellClicked', self.handle_cell_click)
                    
                    # 查询控制按钮
                    with ui.row().classes('w-full justify-between mt-4'):
                        with ui.row().classes('items-center gap-4'):
                            self.query_size = ui.number('显示数量(<100)', value=10, min=1, max=100).classes('w-32 tech-input')
                            self.scroll_checkbox = ui.checkbox('使用游标查询(大量数据)', value=False)
                        
                        with ui.row().classes('gap-3'):
                            ui.button('🗑️ 清除条件', on_click=self.clear_conditions).classes('clear-button')
                            ui.button('📄 显示原始查询', on_click=self.show_raw_query).classes('clear-button')
                            ui.button('🔍 执行查询', on_click=self.execute_query).classes('clear-button')
            
            #查询结果
            with ui.column().classes('w-full gap-4').style('min-height: 600px;'): 
                with ui.card().classes('w-full custom-border p-4').style('min-height: 590px;'):

                    with ui.row().classes('w-full items-center justify-between'):
                        ui.label('📋 查询结果').classes('text-h6 font-bold custom-primary')
                        with ui.row().classes('items-center gap-3'):
                            self.export_status_label=ui.label('导出状态:待执行').classes('text-h6 font-bold custom-primary')
                            self.result_stats = ui.label('').classes('text-sm stats-badge')
                            ui.button('📊 导出全部', on_click=self.export_to_excel, color='positive').classes('export-button')

                    
                    self.result_tabs = ui.tabs().classes('w-full mt-4')

                    with self.result_tabs:
                        self.table_tab = ui.tab('表格视图')
                        self.json_tab = ui.tab('JSON视图')
                        self.query_tab = ui.tab('查询语句')
                        self.fields_tab = ui.tab('字段列表')
                    
                    with ui.tab_panels(self.result_tabs, value=self.table_tab).classes('w-full mt-2 h-480').style('min-height: 550px;'):
                        with ui.tab_panel(self.table_tab):
                            self.result_table = ui.aggrid({
                                'columnDefs': [],
                                'rowData': [],
                                'rowHeight': 45,
                                'theme': 'ag-theme-alpine'
                            }).classes(' w-full').style('min-height: 550px;')
                        
                        with ui.tab_panel(self.json_tab):
                            self.result_json = ui.textarea().classes('w-full font-mono text-sm').style('min-height: 550px;')
                        
                        with ui.tab_panel(self.query_tab):
                            self.query_display = ui.textarea().classes('w-full font-mono text-sm').style('min-height: 550px;')
                            
                        with ui.tab_panel(self.fields_tab):
                            self.fields_list = ui.aggrid({
                                'columnDefs': [{'headerName': '字段名', 'field': 'field', 'flex': 1}],
                                'rowData': [],
                                'rowHeight': 40,
                                'theme': 'ag-theme-alpine'
                            }).classes('w-full').style('min-height: 550px;')

        # 原始查询对话框
        with ui.dialog() as self.raw_query_dialog, ui.card().classes('w-2/3 custom-border p-6'):
            ui.label('📄 原始查询语句').classes('text-h6 font-bold custom-primary mb-4')
            self.raw_query_display = ui.textarea().classes('w-full font-mono text-sm tech-input')
            ui.button('❌ 关闭', on_click=self.raw_query_dialog.close).classes('clear-button mt-4')
            
        # 添加Ag-Grid按钮渲染器的JavaScript代码
        ui.add_head_html('''
            <script>
            class ButtonRenderer {
                init(params) {
                    this.params = params;
                    this.eGui = document.createElement('button');
                    this.eGui.innerHTML = params.label || '按钮';
                    this.eGui.className = params.className || 'ag-button';
                    this.eGui.onclick = () => {
                        // 触发自定义事件
                        const event = new CustomEvent('buttonClick', {
                            detail: {
                                rowIndex: params.rowIndex,
                                data: params.data
                            }
                        });
                        this.eGui.dispatchEvent(event);
                    };
                }
                getGui() {
                    return this.eGui;
                }
                refresh() {
                    return false;
                }
            }
            </script>
        ''')
    
    def handle_cell_click(self, e):
        """处理单元格点击事件"""
        if e.args.get('colId') == 'action':  # 检查是否点击了操作列
            row_index = e.args.get('rowIndex')
            if row_index is not None:
                self.remove_condition(row_index)
    
    def setup_query_value_input(self):
        """设置查询值输入控件"""
        # 清除现有内容
        self.query_value_container.clear()
        
        query_type = self.query_type.value
        
        if query_type == 'range':
            # 范围查询需要两个值
            with self.query_value_container:
                with ui.row().classes('w-full items-center gap-2'):
                    # ui.label('从:').classes('w-10 text-secondary font-medium')  .style("line-height: 35px!important;")
                    self.range_gte = ui.input(placeholder='最小值').classes('flex-1 tech-input  range-input')
                    # ui.label('到:').classes('w-10 text-secondary font-medium')
                    self.range_lte = ui.input(placeholder='最大值').classes('flex-1 tech-input range-input')
        elif query_type == 'exists':
            # 存在查询不需要值
            with self.query_value_container:
                ui.label('检查字段是否存在').classes('text-secondary italic font-medium').style("line-height: 57px!important;")
        else:
            # 其他查询类型只需要一个值
            with self.query_value_container:
                self.query_value = ui.input('查询值', placeholder='输入查询值...').classes('w-full tech-input')
    
    def on_query_type_change(self, e):
        """当查询类型改变时更新输入控件"""
        self.setup_query_value_input()
    
    def connect_es(self):
        """连接 Elasticsearch"""
        host = self.host_input.value
        username = self.username_input.value
        password = self.password_input.value
        
        try:
            # 构建连接参数
            es_params = {'hosts': [host], 'timeout': 5}
            if username and password:
                es_params['http_auth'] = (username, password)
            
            # 测试连接
            self.es = Elasticsearch(**es_params)
            if self.es.ping():
                self.connection_status.text = '● 已连接'
                self.connection_status.classes(replace='connection-status status-connected')
                self.refresh_indices()
                self.custom_notify(f'🎯 成功连接到 Elasticsearch: {host}', type='positive')
            else:
                self.connection_status.text = '● 连接失败'
                self.connection_status.classes(replace='connection-status status-disconnected')
                self.custom_notify('❌ 连接失败，请检查地址和认证信息', type='negative')
        except ConnectionError:
            self.connection_status.text = '● 连接错误'
            self.connection_status.classes(replace='connection-status status-disconnected')
            self.custom_notify('🔌 连接错误，请检查 Elasticsearch 是否运行', type='negative')
        except Exception as e:
            self.connection_status.text = f'● 错误: {str(e)}'
            self.connection_status.classes(replace='connection-status status-disconnected')
            self.custom_notify(f'⚠️ 连接过程中发生错误: {str(e)}', type='negative')
    
    def refresh_indices(self):
        """刷新索引列表"""
        if not self.es:
            self.custom_notify('请先连接 Elasticsearch', type='warning')
            return
        
        try:
            # 获取所有索引的详细信息
            indices = self.es.cat.indices(format='json', h='index,docs.count,store.size,pri,rep')
            
            # 存储索引统计信息
            self.index_stats = {}
            for idx in indices:
                self.index_stats[idx['index']] = {
                    'docs_count': int(idx.get('docs.count', 0)),
                    'store_size': idx.get('store.size', '0B'),
                    'primary_shards': idx.get('pri', 0),
                    'replica_shards': idx.get('rep', 0)
                }
            
            # 清空索引容器
            self.index_container.clear()
            
            # 添加索引卡片 - 清晰设计
            for idx in indices:
                index_name = idx['index']
                stats = self.index_stats[index_name]
                
                with self.index_container:
                    with ui.card().classes(f'index-card') as card:
                        # 存储索引名称到卡片元素中
                        card._props['data-index'] = index_name
                        
                        with ui.column().classes('w-full'):
                            # 索引名称和操作按钮
                            with ui.row().classes('w-full items-center justify-between mb-2'):
                                ui.label(index_name).classes('font-bold text-lg custom-primary flex-1 truncate')
                                ui.button(
                                    icon='visibility', 
                                    on_click=lambda i=index_name: self.select_index(i)
                                ).classes('action-button')
                            
                            # 文档总数 - 清晰显示
                            ui.label(f"{stats['docs_count']:,} 文档").classes('doc-count-highlight')
                            
                            # 索引统计信息网格
                            with ui.column().classes('w-full text-sm gap-2'):
                                with ui.row().classes('w-full justify-between'):
                                    ui.label(f"分片: {stats['primary_shards']}/{stats['replica_shards']}").classes('text-secondary font-medium')
                                    ui.label(f"大小: {self.format_size(stats['store_size'])}").classes('text-secondary font-medium')         
                                # # 索引类型标签
                                # index_type = '系统' if index_name.startswith('.') else '用户'
                                # ui.label(index_type).classes(
                                #     f"index-type-badge {'system-index' if index_type == '系统' else 'user-index'}"
                                # )
            
            # 如果有当前选中的索引，高亮显示
            if self.current_index:
                self.highlight_selected_index()
            
        except Exception as e:
            self.custom_notify(f'获取索引列表失败: {str(e)}', type='negative')
    
    def filter_indices(self):
        """过滤索引列表"""
        search_term = self.index_search.value.lower()
        
        # 获取所有索引卡片
        for card in self.index_container:
            if hasattr(card, '_props') and 'data-index' in card._props:
                index_name = card._props['data-index']
                if search_term in index_name.lower():
                    card.style('display: flex;')
                else:
                    card.style('display: none;')
    
    def highlight_selected_index(self):
        """高亮显示选中的索引"""
        # 获取所有索引卡片
        for card in self.index_container:
            if hasattr(card, '_props') and 'data-index' in card._props:
                index_name = card._props['data-index']
                if index_name == self.current_index:
                    card.classes(add='selected-index')
                else:
                    card.classes(remove='selected-index')
    
    def format_size(self, size_str):
        """格式化大小显示"""
        if not size_str or size_str == 'null':
            return '0B'
        return size_str
    
    def select_index(self, index_name):
        """选择索引"""
        self.current_index = index_name
        
        # 更新当前索引标签
        self.current_index_label.text = f"📁 当前索引: {index_name}"
        
        # 更新文档计数
        if index_name in self.index_stats:
            stats = self.index_stats[index_name]
            self.index_doc_count.text = f"文档总数: {stats['docs_count']:,}"
        
        # 高亮显示选中的索引
        self.highlight_selected_index()
        
        # 获取索引的字段映射
        self.refresh_fields()
        
        self.custom_notify(f'🎯 已选择索引: {index_name}', type='info')
    
    def refresh_fields(self):
        """刷新字段列表"""
        if not self.current_index:
            return
        
        try:
            # 获取索引的映射信息
            mapping = self.es.indices.get_mapping(index=self.current_index)
            fields = self.extract_fields_from_mapping(mapping)
            
            # 更新字段列表
            self.fields = fields
            
            # 更新查询构建器的字段选择
            self.query_field.options = fields
            self.query_field.update()
            
            # 更新字段列表标签页
            self.fields_list.options['rowData'] = [{'field': field} for field in fields]
            self.fields_list.update()
            
        except NotFoundError:
            self.custom_notify(f'索引 {self.current_index} 不存在', type='negative')
        except Exception as e:
            self.custom_notify(f'获取字段列表失败: {str(e)}', type='negative')
    
    def extract_fields_from_mapping(self, mapping):
        """从映射信息中提取字段名"""
        fields = []
        
        # 递归提取字段
        def extract_fields(properties, prefix=''):
            for field, props in properties.items():
                full_field = f"{prefix}{field}"
                if 'properties' in props:
                    # 嵌套字段
                    extract_fields(props['properties'], f"{full_field}.")
                else:
                    # 普通字段
                    fields.append(full_field)
        
        # 获取第一个索引的映射（通常只有一个）
        index_name = list(mapping.keys())[0]
        if 'mappings' in mapping[index_name]:
            mappings = mapping[index_name]['mappings']
            if 'properties' in mappings:
                extract_fields(mappings['properties'])
            # 处理 Elasticsearch 7.x 和 8.x 的差异
            elif hasattr(mappings, 'get') and mappings.get('properties'):
                extract_fields(mappings['properties'])
        
        return sorted(fields)
    
    def add_query_condition(self):
        """添加查询条件"""
        field = self.query_field.value
        query_type = self.query_type.value
        
        if not field:
            self.custom_notify('请选择字段', type='warning')
            return
        
        # 根据查询类型获取值
        if query_type == 'range':
            gte = self.range_gte.value
            lte = self.range_lte.value
            if not gte and not lte:
                self.custom_notify('请至少输入一个范围值', type='warning')
                return
            value = f"gte: {gte or '*'}, lte: {lte or '*'}"
        elif query_type == 'exists':
            value = "字段存在检查"
        else:
            value = self.query_value.value
            if not value:
                self.custom_notify('请输入查询值', type='warning')
                return
        
        # 添加条件到内部列表
        condition_id = len(self.query_conditions_list)
        new_condition = {
            'id': condition_id,
            'field': field,
            'type': query_type,
            'value': value
        }
        self.query_conditions_list.append(new_condition)
        
        # 更新UI表格
        self.update_conditions_table()
        
        # 清空输入
        if query_type == 'range':
            self.range_gte.value = ''
            self.range_lte.value = ''
        elif query_type != 'exists':
            self.query_value.value = ''
    
    def update_conditions_table(self):
        """更新查询条件表格"""
        row_data = []
        for i, condition in enumerate(self.query_conditions_list):
            row_data.append({
                'id': condition['id'],
                'field': condition['field'],
                'type': condition['type'],
                'value': condition['value'],
                'action': '删除'
            })
        
        self.query_conditions.options['rowData'] = row_data
        self.query_conditions.update()
    
    def remove_condition(self, condition_id):
        """删除查询条件"""
        if 0 <= condition_id < len(self.query_conditions_list):
            self.query_conditions_list.pop(condition_id)
            # 重新分配ID
            for i, condition in enumerate(self.query_conditions_list):
                condition['id'] = i
            
            self.update_conditions_table()
            self.custom_notify('✅ 查询条件已删除', type='positive')
    
    def clear_conditions(self):
        """清除所有查询条件"""
        self.query_conditions_list = []
        self.update_conditions_table()
        self.custom_notify('🗑️ 所有查询条件已清除', type='info')
    
    def build_query(self):
        """构建查询DSL"""
        if not self.query_conditions_list:
            # 如果没有条件，返回匹配所有文档的查询
            return {'query': {'match_all': {}}}
        
        # 构建布尔查询
        must_conditions = []
        
        for condition in self.query_conditions_list:
            field = condition['field']
            query_type = condition['type']
            value = condition['value']
            
            if query_type == 'match':
                must_conditions.append({'match': {field: value}})
            elif query_type == 'term':
                must_conditions.append({'term': {field: value}})
            elif query_type == 'range':
                # 解析范围查询
                range_query = {}
                if 'gte:' in value and 'lte:' in value:
                    gte_part, lte_part = value.split('lte:')
                    gte_val = gte_part.replace('gte:', '').replace(",","").strip()
                    lte_val = lte_part.strip()
                    
                    if gte_val != '*':
                        range_query['gte'] = gte_val
                    if lte_val != '*':
                        range_query['lte'] = lte_val
                
                must_conditions.append({'range': {field: range_query}})
            elif query_type == 'wildcard':
                must_conditions.append({'wildcard': {field: value}})
            elif query_type == 'exists':
                must_conditions.append({'exists': {'field': field}})
        
        return {'query': {'bool': {'must': must_conditions}}}
    
    def execute_query(self):
        """执行查询"""
        if not self.current_index:
            self.custom_notify('请先选择一个索引', type='warning')
            return
        
        try:
            # 构建查询
            query = self.build_query()
            size = int(self.query_size.value)
            
            # 显示查询语句
            self.query_display.value = json.dumps(query, indent=2, ensure_ascii=False)
            
            # 执行查询
            if self.scroll_checkbox.value and size > 100:
                # 使用游标查询大量数据
                result = self.es.search(
                    index=self.current_index,
                    body=query,
                    scroll='2m',
                    track_total_hits=True,
                    size=min(size, 5000)  # 游标查询单次最多1000条
                )
            else:
                # 普通查询
                result = self.es.search(
                    index=self.current_index,
                    body=query,
                    track_total_hits=True,
                    size=size
                )
            
            # 存储当前查询结果
            self.current_query_result = result
            
            # 显示结果
            self.display_results(result)
            
        except Exception as e:
            errStr=f'查询执行失败: {str(e)}'
            print(errStr)
            self.custom_notify(errStr, type='negative')
    
    def display_results(self, result):
        """显示查询结果"""
        hits = result.get('hits', {}).get('hits', [])
        total = result.get('hits', {}).get('total', {})
        
        # 显示结果统计
        if isinstance(total, dict):
            total_count = total.get('value', len(hits))
        else:
            total_count = total
            
        self.result_stats.text = f'找到 {total_count} 条结果，显示 {len(hits)} 条'
        
        if not hits:
            # 清空结果
            self.result_table.options['columnDefs'] = []
            self.result_table.options['rowData'] = []
            self.result_table.update()
            self.result_json.value = '未找到匹配的文档'
            return
        
        # 表格视图
        # 提取所有字段作为列
        columns_set = set()
        for hit in hits:
            if '_source' in hit:
                columns_set.update(hit['_source'].keys())
        
        # 添加_id和_score作为固定列
        columns_set.add('_id')
        columns_set.add('_score')
        
        columns = sorted(list(columns_set))
        column_defs = [{'headerName': col, 'field': col, 'flex': 1} for col in columns]
        
        # 准备行数据
        row_data = []
        for hit in hits:
            row = hit.get('_source', {}).copy()
            row['_id'] = hit['_id']  # 添加文档ID
            row['_score'] = hit.get('_score', 0)  # 添加评分
            row_data.append(row)
        
        # 更新表格
        self.result_table.options['columnDefs'] = column_defs
        self.result_table.options['rowData'] = row_data
        self.result_table.update()
        
        # JSON视图
        self.result_json.value = json.dumps(result, indent=2, ensure_ascii=False)
    
    def show_raw_query(self):
        """显示原始查询语句"""
        query = self.build_query()
        self.query=query
        self.raw_query_display.value = json.dumps(query, indent=2, ensure_ascii=False)
        self.raw_query_dialog.open()
    
    def export_to_excel(self):
        print("主事件循环",self.main_loop)
        self.update_export_status(f'待导出...')

        thread = threading.Thread(target=self.export_to_excel_in_thread)
        thread.daemon = True
        thread.start()

    def export_to_excel_in_thread(self):
        """导出数据到Excel"""
        if not self.current_query_result:
            self.async_notify('请先执行查询', type='warning')
            self.update_export_status('请先执行查询', type='warning')
            return
        hits = self.current_query_result.get('hits', {}).get('hits', [])
        if not hits:
            self.async_notify('没有数据可导出', type='warning')
            self.update_export_status('没有数据可导出', type='warning')
            return
        if len(hits)>1000000:
            self.async_notify('导出数据不可超过100w', type='warning')
            self.update_export_status('导出数据不可超过100w', type='warning')

            return
        try:
            # 准备数据

            docs = scan(
                self.es,
                index=self.current_index,
                query=self.query,
                scroll="1m"
            )
            listData=[]
            self.async_notify('数据查询中，请耐心等待', type='positive')
            ii=0
            self.update_export_status(f'数据下载中...')
            for doc in docs:
                doc_id = doc["_id"]
                ii=ii+1
                source = doc["_source"]
                source['id'] = doc_id  # 添加文档ID到源数据中
                source = {'id': doc_id, **source} 
                listData.append(source)
            self.update_export_status(f'数据导出中，共{ii}条')

            # 创建DataFrame
            df = pd.DataFrame(listData[0:], columns=listData[0])
            # 创建Excel文件
            self.async_notify('数据导出中，请耐心等待', type='positive')
            
            # 生成文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{self.current_index}_export_{timestamp}.xlsx"
            df.to_excel("./"+filename, index=False)

            # 提供下载
            self.update_export_status(f'数据已导出，共{ii}条')
            self.async_notify(f'✅ 数据已导出到 {filename},共{ii}条', type='positive')
            
        except Exception as e:
            print("导出失败:",e)
            self.async_notify(f'导出Excel失败: {str(e)}', type='negative')

# 创建应用实例

# # 启动应用
# if __name__ in {"__main__", "__mp_main__"}:
#     app = ElasticsearchHead()

#     ui.run(title="🔍 Elasticsearch Head 客户端", port=8080, reload=False, dark=None)
@ui.page('/')          # 也可以 /home
def index():
 # 监听所有输入变化并缓存

    # # 页面级守卫：没 token直接跳
    # if not ng_app.storage.user.get('token'):
    #     ui.navigate.to('/login')
    #     return
    myapp = ElasticsearchHead()
    # myapp.create_ui()
    # 设置页面样式
    myapp.setup_styles()
    # 创建主界面
    myapp.setup_ui()


ui.run(title="py-es-head", 
    storage_secret="SECRET",
    reload=False,
    native=False, 
    port=17766, 
    window_size=(1024, 768),   # ← 宽, 高
    # fullscreen=False
)