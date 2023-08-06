import requests

CONTEXT_PROMPT = "啥也不干"
CLASSIFICATION_PROMPT = "01.打开搜索页面 02.关闭当前页面 03.执行PC按键输入组合脚本"
ROLE_PROMPT = "你是一名电脑助手，根据以下句子，分析用户的意图，并回复相应的类型，类型为：" + CLASSIFICATION_PROMPT + "。只需要回答数字即可，以下为要分析的句子：\n"

proxies = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890'
}
def gpt_recognize(context: str):
    url = "https://api.openai-proxy.com/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer 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"
    }
    data = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": context}]
    }

    response = requests.post(url, headers=headers, json=data,proxies=proxies)
    return response.json()


def gpt_recognize_types(context=CONTEXT_PROMPT):
    rec = gpt_recognize(ROLE_PROMPT + context)
    return rec


def gpt_action_aim_analyze(context):
    role_prompt = "你好 \n"
    rec = gpt_recognize(role_prompt + context)
    if rec == "None":
        rec = "baidu.com"
    return rec


if __name__ == '__main__':
    while 1:
        context = input('请输入问题:')
        print(f"ans:{gpt_action_aim_analyze(context)['choices'][0]['message']['content']}")

