import pandas as pd
import IPython.display as ip_disp


def max_data_frame_columns(n=None):
    pd.set_option('display.max_columns', n)

def display(obj):
    ip_disp.display(obj)

def display_html(html):
    ip_disp.display(ip_disp.HTML(html))

def display_md(md):
    ip_disp.display(ip_disp.Markdown(md))

def md(txt, size="#", color="black"):
    md_text = f"{size} <font color={color}>{txt}</font>"
    display_md(md_text)

def printt(txt, size=40, color="black"):
    html_font = f'<font style="font-size:{size}px" color={color}>{txt}</font>'
    display_html(html_font)

def hr():
    display_html('<hr style="height:1px;border:none;color:#333;background-color:#333;" />')





