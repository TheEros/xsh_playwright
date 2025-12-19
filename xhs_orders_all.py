import time
import json
import csv
import requests
from urllib.parse import urlencode

BASE_URL = "https://pgy.xiaohongshu.com/api/solar/order/task/query"


# 你原来的查询参数，建议用 params 传，别拼到 url 里
BASE_PARAMS = {
    "coPlatform": "",
    "cooperationType": "",
    "isWindmill": "false",
    "kolType": "0",
    "kolUserId": "",
    "mcnAuthorized": "",
    "negotiationStatus": "",
    "orderId": "",
    "pageNum": 1,
    "pageSize": 20,
    "reportBrandUserId": "",
    "settlementRule": "",
    "state": "",
    "title": "",
    "windmillFilter": 0,
}


def fetch_page(
    session: requests.Session, page_num: int, retries: int = 3, sleep_sec: float = 0.6
):
    params = dict(BASE_PARAMS)
    params["pageNum"] = page_num

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(
                    f"API code != 0, code={data.get('code')}, msg={data.get('msg')}"
                )
            # 轻微限速，避免太快触发风控

            time.sleep(sleep_sec)
            return data
        except Exception as e:
            last_err = e

            # 递增退避
            time.sleep(sleep_sec * attempt)

    raise last_err


def main(cookies: dict, headers: dict):
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update(headers)

    # 先拉第一页，拿 totalPage
    first = fetch_page(session, 1)
    payload = first.get("data") or {}
    total_page = int(payload.get("totalPage") or 1)

    all_tasks = []
    all_tasks.extend(payload.get("list") or [])

    # 拉剩余页
    for p in range(2, total_page + 1):
        page_data = fetch_page(session, p)
        page_payload = page_data.get("data") or {}

        all_tasks.extend(page_payload.get("list") or [])
        print(f"Fetched page {p}/{total_page}, tasks_total={len(all_tasks)}")

    # 1) 保存原始 task 列表结构
    with open("xhs_tasks_all.json", "w", encoding="utf-8") as f:
        json.dump(all_tasks, f, ensure_ascii=False, indent=2)

    # 2) 展平 orderVos，保存 CSV
    rows = []
    for task in all_tasks:
        task_no = task.get("taskNo")

        title = task.get("title")

        report_brand_user_name = task.get("reportBrandUserName")
        expect_publish_time = task.get("expectPublishTime")

        for order in task.get("orderVos") or []:
            row = {
                "taskNo": task_no,
                "taskTitle": title,
                "reportBrandUserName": report_brand_user_name,
                "expectPublishTime": expect_publish_time,
                # order 字段（按需增删）
                "orderId": order.get("orderId"),
                "totalPrice": order.get("totalPrice"),
                "contentPrice": order.get("contentPrice"),
                "createTime": order.get("createTime"),
                "notePublishTime": order.get("notePublishTime"),
                "orderStatus": order.get("orderStatus"),
                "state": order.get("state"),
                "contentType": order.get("contentType"),
                "settlementRule": order.get("settlementRule"),
                "needAdsAudit": order.get("needAdsAudit"),
                "kolId": order.get("kolId"),
                "kolName": order.get("kolName"),
                "brandId": order.get("brandId"),
                "brandName": order.get("brandName"),
            }
            rows.append(row)

    csv_path = "xhs_orders_all.csv"
    if rows:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))

            writer.writeheader()
            writer.writerows(rows)
    else:
        # 没有 orderVos 的话也生成空表头文件
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            f.write("")

    print(
        f"\nDone.\n- tasks saved to: xhs_tasks_all.json ({len(all_tasks)} tasks)\n- orders saved to: {csv_path} ({len(rows)} rows)"
    )


cookies = {
    "abRequestId": "62ddc872-6aa4-52be-a801-9bdc7df8569b",
    "a1": "19768a94168phvedmkahlx6lxrfyua04srms8cggo50000220309",
    "webId": "86ef5f0e778ead5fe706f55202a2faf5",
    "gid": "yjWKY0j2JiM0yjWKY0j4yFSkKYUxhdf6T0x16klCK1CFiM28v7084Y888JJ8q8j8Djfdj2f0",
    "x-user-id-creator.xiaohongshu.com": "618f62ac000000001000d08b",
    "customerClientId": "135290779466254",
    "x-user-id-zhaoshang.xiaohongshu.com": "6676ec440000000007006f9e",
    "x-user-id-pro.xiaohongshu.com": "618f62ac000000001000d08b",
    "x-user-id-ad.xiaohongshu.com": "59ebefa3e8ac2b2171a39d89",
    "web_session": "040069b0fb21e41280dd4eb3053b4bd730c1f1",
    "x-user-id-ark.xiaohongshu.com": "6676ec440000000007006f9e",
    "access-token-ark.xiaohongshu.com": "customer.ark.AT-68c5175828067488533217313tlj0kh8ud9qb6m5",
    "x-user-id-school.xiaohongshu.com": "6676ec440000000007006f9e",
    "customer-sso-sid": "68c517584714448772071428dga5kvvbjxsqctdi",
    "x-user-id-pgy.xiaohongshu.com": "59ebefa3e8ac2b2171a39d89",
    "solar.beaker.session.id": "AT-68c517584714448772055043ydedmb876fqcofkh",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh",
    "acw_tc": "0a4252f017660202812401339e015623b1ea51bf4f9661a60c5530d562ab98",
    "websectiga": "16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134",
    "sec_poison_id": "850f0bd6-5311-4371-9268-ed2e073b609a",
    "xsecappid": "ratlin",
    "loadts": "1766020309833",
}

cookies = {
    "abRequestId": "68c22f1e-0b84-5747-ab8d-561388a02aaf",
    "a1": "19777849453hjdlpofzpcrg02hhxzgf11dg1g11li50000112937",
    "webId": "6acae39285e8fb70dd85223208d3e28d",
    "web_session": "030037af7cebd047f401777b292f4a908cbc24",
    "gid": "yjWY8jySSiyqyjWWWY4j4fyy2qxVf1UliuUSk13Fk8Jxxy28Cukiyk888yyJjqW8q4Dij2yJ",
    "customerClientId": "780284401245141",
    "x-user-id-ark.xiaohongshu.com": "59ebefa3e8ac2b2171a39d89",
    "x-user-id-creator.xiaohongshu.com": "618f62ac000000001000d08b",
    "access-token-ark.xiaohongshu.com": "customer.ark.AT-68c517584284453826363397oa9lg7iui51k6wjq",
    "beaker.session.id": "fb99386818a506ceea46be2abef3cbfcc24667b7gAJ9cQAoWAsAAABhcmstbGlhcy1pZHEBWBgAAAA2N2JmMTFhNDhjMjViMzAwMTU0ZTM3MTRxAlgOAAAAcmEtdXNlci1pZC1hcmtxA1gYAAAANjdiZjExYTQwNGYwMDAwMDAwMDAwMDAycQRYDgAAAF9jcmVhdGlvbl90aW1lcQVHQdpQMqH2dslYEQAAAHJhLWF1dGgtdG9rZW4tYXJrcQZYQQAAAGVlNWFlMWEyMWM1NDRlZWFhYThlYTNjMWVhZjMzZjcwLTkzYTk5NWM5NzA4OTRjOWNhZWY3MTQwMjljODU5YjE3cQdYAwAAAF9pZHEIWCAAAABmMzBiYTM2ZDQzZTQ0ZjJiOWRlYWNmNTg0OGYyZjk2MHEJWA4AAABfYWNjZXNzZWRfdGltZXEKR0HaUDKh9nbJdS4=",
    "acw_tc": "0a422b7a17660382657197806ee3f3044b9e7c025f0c8b42582a2f4f1122c6",
    "xsecappid": "ratlin",
    "websectiga": "cf46039d1971c7b9a650d87269f31ac8fe3bf71d61ebf9d9a0a87efb414b816c",
    "sec_poison_id": "97f9d940-31a8-4368-846b-2570c5bff7e6",
    "customer-sso-sid": "68c517585080865316962312hmahbcvy59d7x51y",
    "x-user-id-pgy.xiaohongshu.com": "62bd9327000000001b0257a1",
    "solar.beaker.session.id": "AT-68c517585080865316978690hjawvsyp9agpvgnx",
    "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c517585080865316978690hjawvsyp9agpvgnx",
    "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c517585080865316978690hjawvsyp9agpvgnx",
    "loadts": "1766039262522",
}
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "authorization": "",
    "priority": "u=1, i",
    "referer": "https://pgy.xiaohongshu.com/solar/transaction_v2/brand/order-list/kol",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-b3-traceid": "ee8596a126f5fd16",
    "x-s": "Z21lZjdkOlOksjVBOjaU021+OBM+OjcLOjFiOj5GZBF3",
    "x-s-common": "2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1PahFHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQUGgzVynhTq9YSJBIjNsQh+sHCH0Z1PshlPeLAHjIj2eLjwjHlw/q9wBrE+er9womi4fpDJnTYyBlh+flhqf8E4nrI+o+UJgPhG94dJALIPeZIP0HIPAZEHjIj2eGjwjHjNsQh+UHCHjHVHdWhH0ija/PhqDYD87+xJ7mdag8Sq9zn494QcUT6aLpPJLQy+nLApd4G/B4BprShLA+jqg4bqD8S8gYDPBp3Jf+m2DMBnnEl4BYQyrkSLFQ+zrTM4bQQPFTAnnRUpFYc4r4UGSGILeSg8DSkN9pgGA8SngbF2pbmqbmQPA4Sy9Ma+SbPtApQy/8A8BES8p+fqpSHqg4VPdbF+LHIzrQQ2sTczFzkN7+n4BTQ2BzA2op7q0zl4BSQyopYaLLA8/+Pp0mQPM8LaLP78/mM4BIUcLzTqFl98Lz/a7+/LoqMaLp9q9Sn4rkOqgqhcdp78SmI8BpLzS4OagWFprSk4/8yLo4ULopF+LS9JBbPGf4AP7bF2rSh8gPlpd4HanTMJLS3agSSyf4AnaRgpB4S+9p/qgzSNFc7qFz0qBSI8nzSngQr4rSe+fprpdqUaLpwqM+l4Bl1Jb+M/fkn4rS9J9p3qgcAGMi7qM86+B4Qzp+EanYb2LE6nSpQ2BMc/7kTJFSkGMYSLo4Bag8kcAQs+7+r204A2b8F8rS9+fpD+A4SnnkMtFSk+nL9p9Qr/db7yDSiadPAJ9pA+Dz98pzc4rE0p/8Szop7+LS9nnECN9zS8ob74dms87+/Lo4Ta/+LJDShJnlAqgzAGpm72g+AaBQycg8SP7Z68Lzc47YQc9pAyFcI8p+SaBSQy/4AyfQ6qA+dqD8QynzS+dp7PFSkqfTF4gzT4opFzDSh8npLLozBanYt8nSn4BlQy94SPbmFLUTl4MmCLo4pag8iyFShG7HF8FYmtFSw8nSc4BkQyoQFanY6q9kl4bmPqbbY47QNq9zfG9zQyoi6Pdp7ndQM4M+Qc9zAyMmFLUT+a9LlGFbAPgQ/2LSicg+hcDRAPgbFqFQn4FEQ4f4S8S87aLS389L9Pb86aL+N8nkDN7+fLozpagW7q7YM4FcFcSSYanYz8FS9+gPl//8S+f49qA8VP9pfpd48anYVaLS9qfi6Lo4Ha/+MnLSk8Bpk20Yrz9Ed8gYl47mQyF4z/bm7+LEBcnp820pA8S8FnjRl4BRQP9SDanWharQM4e4cqg4Ha/+Oq9k0/9pLqgq6adpF4DS9t9lHpdzcanTgzsT0pdYQ2emAyFzmqA+j+9pg4g4pcdbFLDShqDSQyLRA8opFnDSeafp3J0WMaL+/GfRg2DYQ4fl7aLP98p+c4Fz0pdzwabmF+DDAyr+QzLbAyMLA8p8c4MQ0qBYhag8U4D4M4oQSqgzTa/+o+LDALDkQyFLAanYw8p818gPl/pzk/M87aFSezFpIqg4TnSm7wLSbafp8qBYNagYo2db6PBp/qgzeqSmFJ9pparSH/BSsa/+jNAYl49zQPA8ApdkCqFSkJnYspd4banVh2aTc4B8yqgzPaLprnLSenLMQc7kH/7p74DS3/sRQz/+APgb7zBMyLDSQcMklaL+9q9zf/7+kqg4Mag8b+FSizF8PJb43aL+jLLly4nzQ408AnLMm8p+M4MmQyrRApbmFaFSbnflQcA+Azob72DS9LFEj4g4raLPM8nS0q/FjNsQhwaHCN/HlPAHE+0ZAweDVHdWlPsHCPsIj2erlH0ijJBSF8aQR",
    "x-t": "1766020310634",
    # 'cookie': 'abRequestId=62ddc872-6aa4-52be-a801-9bdc7df8569b; a1=19768a94168phvedmkahlx6lxrfyua04srms8cggo50000220309; webId=86ef5f0e778ead5fe706f55202a2faf5; gid=yjWKY0j2JiM0yjWKY0j4yFSkKYUxhdf6T0x16klCK1CFiM28v7084Y888JJ8q8j8Djfdj2f0; x-user-id-creator.xiaohongshu.com=618f62ac000000001000d08b; customerClientId=135290779466254; x-user-id-zhaoshang.xiaohongshu.com=6676ec440000000007006f9e; x-user-id-pro.xiaohongshu.com=618f62ac000000001000d08b; x-user-id-ad.xiaohongshu.com=59ebefa3e8ac2b2171a39d89; web_session=040069b0fb21e41280dd4eb3053b4bd730c1f1; x-user-id-ark.xiaohongshu.com=6676ec440000000007006f9e; access-token-ark.xiaohongshu.com=customer.ark.AT-68c5175828067488533217313tlj0kh8ud9qb6m5; x-user-id-school.xiaohongshu.com=6676ec440000000007006f9e; customer-sso-sid=68c517584714448772071428dga5kvvbjxsqctdi; x-user-id-pgy.xiaohongshu.com=59ebefa3e8ac2b2171a39d89; solar.beaker.session.id=AT-68c517584714448772055043ydedmb876fqcofkh; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh; acw_tc=0a4252f017660202812401339e015623b1ea51bf4f9661a60c5530d562ab98; websectiga=16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134; sec_poison_id=850f0bd6-5311-4371-9268-ed2e073b609a; xsecappid=ratlin; loadts=1766020309833',
}

# 你已在外部准备好了 cookies / headers 的话，直接调用：
main(cookies=cookies, headers=headers)
