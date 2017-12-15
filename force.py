urls = [
    "设备在线数量",
    "购买情况/次数/天",
    "购买情况/次数/季度",
    "购买情况/次数/年",
    "购买情况/平均金额/天",
    "购买情况/平均金额/季度",
    "购买情况/平均金额/年",
    "换台次数",
    "用户在线天数",
    "收视率"
]

import os
import urllib.parse
for url in urls:
    print(url)
    os.system("curl http://123.206.18.178/{} >/dev/null".format(urllib.parse.quote(url)))
