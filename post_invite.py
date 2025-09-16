import time
import requests

cookies = {
    "a1": "19841b7dbc2g4lp8jzlz23cqyhhzikeupa7rpm2lr30000355340",
    "webId": "7bacca47ddd6d7a320bb3130e5c4c7a7",
    "gid": "yjY4yDWddSUDyjY4yDWfD06JSJk41UYVu1uJW1FqSAvxxFq8u3Td7U888q22q488dDidY2Ki",
    "customerClientId": "132884442829052",
    "abRequestId": "7bacca47ddd6d7a320bb3130e5c4c7a7",
    "web_session": "04006976ad20a7f1cddf1eb0a83a4b9d18e288",
    "acw_tc": "0a42543217580267832586971e5424af52d7640134cda253c26ef52b46cb2f",
    "customer-sso-sid": "68c517550667680375341062jrwinjykep5feekj",
    "solar.beaker.session.id": "AT-68c5175506676846698496006whgtshmwluc059k",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k",
    "xsecappid": "ratlin",
    "websectiga": "3633fe24d49c7dd0eb923edc8205740f10fdb18b25d424d2a2322c6196d2a4ad",
    "sec_poison_id": "33ce602c-6988-4ed7-9a1e-6aaadb1ea7dd",
    "loadts": "1758027554710",
}

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en,zh-CN;q=0.9,zh;q=0.8",
    "authorization": "",
    "content-type": "application/json;charset=UTF-8",
    "origin": "https://pgy.xiaohongshu.com",
    "priority": "u=1, i",
    "referer": "https://pgy.xiaohongshu.com/solar/pre-trade/invite-form?id=5f3bd40a0000000001000a65&trackId=",
    "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "x-b3-traceid": "23e3c3022ddeec7f",
    "x-s": "12qUO2siOjFi0gcKsgFG12ZBOjsL021lO6ZBZgA+O6F3",
    "x-s-common": "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",
    "x-t": "1758027565110",
    # 'cookie': 'a1=19841b7dbc2g4lp8jzlz23cqyhhzikeupa7rpm2lr30000355340; webId=7bacca47ddd6d7a320bb3130e5c4c7a7; gid=yjY4yDWddSUDyjY4yDWfD06JSJk41UYVu1uJW1FqSAvxxFq8u3Td7U888q22q488dDidY2Ki; customerClientId=132884442829052; abRequestId=7bacca47ddd6d7a320bb3130e5c4c7a7; web_session=04006976ad20a7f1cddf1eb0a83a4b9d18e288; acw_tc=0a42543217580267832586971e5424af52d7640134cda253c26ef52b46cb2f; customer-sso-sid=68c517550667680375341062jrwinjykep5feekj; solar.beaker.session.id=AT-68c5175506676846698496006whgtshmwluc059k; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c5175506676846698496006whgtshmwluc059k; xsecappid=ratlin; websectiga=3633fe24d49c7dd0eb923edc8205740f10fdb18b25d424d2a2322c6196d2a4ad; sec_poison_id=33ce602c-6988-4ed7-9a1e-6aaadb1ea7dd; loadts=1758027554710',
}
all_rs = [
    "5e53d2020000000001002445",
    "5b0c97fee8ac2b4a0efa2ada",
    "5d394b68000000001600f738",
    "5c6d04eb0000000012006649",
    "5f164d34000000000101cbc5",
    "5bd337b54f79400001d1e22c",
    "62d4c63d000000000303d6c7",
    "61fa75a8000000001000696a",
]
for kolId in all_rs:
    json_data = {
        "kolId": kolId,
        "cooperateBrandName": "次元脉冲",
        "cooperateBrandId": "60ddd0e9000000002002a3a6",
        "productName": "【入坑指南向】rua娃技巧",
        "inviteType": 2,
        "expectedPublishTimeStart": "2025-09-15",
        "expectedPublishTimeEnd": "2025-10-31",
        "inviteContent": "【入坑指南向】rua娃技巧+毛绒养护教程+线下开售倒计时",
        "contactType": 2,
        "contactInfo": "pgy_sens_encrypt:LqnPAevmodSUg/InQWGzQWvJBXrSZrVK4rxIrb6Sf/ZzDk2IJf7DeQUzsyPusq2/1n4MVghdih6FDK0COIv6Wx/h5pvn6Nyjm/Svc0B0xqGtc9NvQq/2V2UFOBLGTOHi",
        "contactInfoCiphertext": "pgy_sens_encrypt:LqnPAevmodSUg/InQWGzQWvJBXrSZrVK4rxIrb6Sf/ZzDk2IJf7DeQUzsyPusq2/1n4MVghdih6FDK0COIv6Wx/h5pvn6Nyjm/Svc0B0xqGtc9NvQq/2V2UFOBLGTOHi",
        "kolType": 0,
        "brandUserId": "60ddd0e9000000002002a3a6",
    }

    response = requests.post(
        "https://pgy.xiaohongshu.com/api/solar/invite/initiate_invite",
        cookies=cookies,
        headers=headers,
        json=json_data,
    )
    print(response.status_code)
    print(response.text)
    time.sleep(1)
