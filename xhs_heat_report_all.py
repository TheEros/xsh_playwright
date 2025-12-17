import requests
import json
import time
from typing import List, Dict, Any, Optional


def fetch_all_heat_reports(
    url: str,
    cookies: dict,
    headers: dict,
    base_payload: dict,
    page_size: int = 50,
    sleep_sec: float = 0.2,
    timeout: int = 20,
    max_retries: int = 3,
) -> List[Dict[str, Any]]:
    """
    分页拉取全部数据，返回聚合后的 data.list
    """

    s = requests.Session()

    s.cookies.update(cookies)

    all_rows: List[Dict[str, Any]] = []
    page_num = 1

    total_page: Optional[int] = None

    while True:
        payload = dict(base_payload)
        payload["pageNum"] = page_num

        payload["pageSize"] = page_size

        last_err = None
        for attempt in range(1, max_retries + 1):
            try:
                r = s.post(url, headers=headers, json=payload, timeout=timeout)
                r.raise_for_status()
                resp = r.json()

                if resp.get("code") != 0 or not resp.get("success", False):
                    raise RuntimeError(
                        f"API返回异常: code={resp.get('code')} msg={resp.get('msg')}"
                    )

                data = resp.get("data") or {}

                if total_page is None:
                    total_page = int(data.get("totalPage") or 0)

                rows = data.get("list") or []
                all_rows.extend(rows)

                print(
                    f"✅ page {page_num}/{total_page}  本页 {len(rows)} 条  累计 {len(all_rows)} 条"
                )
                break  # 成功，退出重试
            except Exception as e:
                last_err = e
                print(f"⚠️ page {page_num} 第{attempt}次失败: {e}")
                time.sleep(0.8 * attempt)

        else:
            raise RuntimeError(
                f"❌ page {page_num} 拉取失败，已重试{max_retries}次: {last_err}"
            )

        # 结束条件：到最后一页，或本页无数据（保险）
        if total_page is not None and page_num >= total_page:
            break

        if not rows:
            break

        page_num += 1
        time.sleep(sleep_sec)

    return all_rows


def save_json(path: str, rows: List[Dict[str, Any]]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def save_csv(path: str, rows: List[Dict[str, Any]]):
    # 用 pandas 更省事；如果你不想依赖 pandas，可以我再给你纯 csv 的版本
    import pandas as pd

    df = pd.DataFrame(rows)

    df.to_csv(path, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
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
        "webBuild": "5.0.7",
        "acw_tc": "0a42136b17659527690507057e5e6da5893a97ce48108c3c2e77f4cd0ac879",
        "unread": "{%22ub%22:%2269383911000000001e014120%22%2C%22ue%22:%22693f570d000000001e0078bf%22%2C%22uc%22:20}",
        "customer-sso-sid": "68c517584714448772071428dga5kvvbjxsqctdi",
        "x-user-id-pgy.xiaohongshu.com": "59ebefa3e8ac2b2171a39d89",
        "solar.beaker.session.id": "AT-68c517584714448772055043ydedmb876fqcofkh",
        "access-token-pgy.xiaohongshu.com": "customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh",
        "access-token-pgy.beta.xiaohongshu.com": "customer.pgy.AT-68c517584714448772055043ydedmb876fqcofkh",
        "xsecappid": "ratlin",
        "websectiga": "16f444b9ff5e3d7e258b5f7674489196303a0b160e16647c6c2b4dcb609f4134",
        "sec_poison_id": "0b5120af-5fc2-4411-9ffb-4a8e3bee50b6",
        "loadts": "1765953996031",
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "authorization": "",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://pgy.xiaohongshu.com",
        "priority": "u=1, i",
        "referer": "https://pgy.xiaohongshu.com/solar/post-trade/content-heat/list",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-b3-traceid": "a11c58837282dd11",
        "x-s": "1iZv0jkkOgdBOl1+Z2sislvlOY5GO2dU1iFCs6slOlT3",
        "x-s-common": "2UQAPsHC+aIjqArjwjHjNsQhPsHCH0rjNsQhPaHCH0c1PahFHjIj2eHjwjQgynEDJ74AHjIj2ePjwjQUGgzVynhTq9YSJBIjNsQh+sHCH0Z1PshlPeLUHjIj2eLjwjHlw/q9wBrE+er9womi4fpDJnTYyBlh+flhqf8E4nrI+o+UJgPhG94dJALIPeZIP0HIPAZEHjIj2eGjwjHjNsQh+UHCHjHVHdWhH0ija/PhqDYD87+xJ7mdag8Sq9zn494QcUT6aLpPJLQy+nLApd4G/B4BprShLA+jqg4bqD8S8gYDPBp3Jf+m2DMBnnEl4BYQyrkS8B8+zrTM4bQQPFTAnnRUpFYc4r4UGSGILeSg8DSkN9pgGA8SngbF2pbmqbmQPA4Sy9Ma+SbPtApQy/8A8BES8p+fqpSHqg4VPdbF+LHIzrQQ2sTczFzkN7+n4BTQ2BzA2op7q0zl4BSQyopYaLLA8/+Pp0mQPM8LaLP78/mM4BIUcLzTqFl98Lz/a7+/LoqMaLp9q9Sn4rkOqgqhcdp78SmI8BpLzS4OagWFprSk4/8yLo4ULopF+LS9JBbPGf4AP7bF2rSh8gPlpd4HanTMJLS3agSSyf4AnaRgpB4S+9p/qgzSNFc7qFz0qBSI8nzSngQr4rSe+fprpdqUaLpwqM+l4Bl1Jb+M/fkn4rS9J9p3qgcAGMi7qM86+B4Qzp+EanYb2LEdappQ2BMc/7kTJFSkGMYSLo4Bag8kcAQs+7+r204A2b8F8rS9+fpD+A4SnnkMtFSk+nL9p9Qr/db7yDSiadPAJ9pA+Dz98pzc4rE0p/8Szop7+LS9nnECPBpS8S87yoZE87+fqgq3a/+L/rShJnlAqgzAGpm72g+AaBQycg8SP7Z68Lzc47YQc9pAyFcI8p+SaBSQy/4AyfQ6qA+dqD8QynzS+dp7PFSkqfTF4gzT4opFzDSh8npLLozBanYt8nSn4BlQy94SPbmFLUTl4MmCLo4pag8iyFShG7HF8FYmtFSw8nSc4BkQyoQFanY6q9kl4bmPqbbY47QNq9zfG9zQyoi6Pdp7ndQM4M+Qc9zAyMmFLUT+a9LlGFbSPBlByrSbPo+h89pAPgbF8FlM47bQcAmS8S8FpLSk/fpLpgpcaL+N8p+safpfqgz/a/+Oq7YM4B8o+MSlaLpktFSe87+3qe8A+0D68/mj+d+DLozCaL+b+DS9GFTP4gqhagYM+DS3+fpnnflk+BhA8nkc4AzQyF8Owop7y7zr89pgng8Ap7bF/f4c4B+QPF8VanYI+o+n4bbHpd4dagW6qM8y8BL9qg4Tqb8FtFSbLpkwqgzcanTHyaTgnDpQ2e4A+dH9q9S+89px4g4iGS8FzDShap8Qy9RA8opFqFS9a9pL+9I3anY/yLYTwrRQcMSFag8N8nTc4MzYqgc3nS8FyFDAye+QyepAp0mO8p8c4Mc6/b4nag8gaoSn4BbALoz3agYkyrDAJ/QQyMPManYt8/+I8g+nL9FlJMm7wrSez9SCpd4PJ7b7zrSi8Bpn+9Y9anSMGFkU8BLl4gzP/MmF2DQCG7QscDz1agY/GFTc4MzQPFRApMpj4LShy9RTpd4xanVF4g4c4bYUqgzsaLpM+FSe20mQcMbT8pm7pLSkLBEQ4DEAyp87znpQn/8Qyb81aL+mqMSrJ7+34g4MaL+azrSbLrp1p7pBanYVa948qLSQcA4AyDMO8p+M4M+QznpAyMmF2DSizfEQc9pAP7p7qrS9tFQjLoqMag8wq9zA/Br6pdzQtM8F2DSeP9pkLozOanYSqM4UJb+QzLRApFlt8/bM4rVjNsQhwaHCP0ZhP/PUwePI+sIj2erIH0iINsQhP/rjwjQVygzSHdF=",
        "x-t": "1765953019028",
        # 'cookie': 'abRequestId=62ddc872-6aa4-52be-a801-9bdc7df8569b; a1=19768a94168phvedmkahlx6lxrfyua04srms8cggo50000220309; webId=86ef5f0e778ead5fe706f55202a2faf5; gid=yjWKY0j2JiM0yjWKY0j4yFSkKYUxhdf6T0x16klCK1CFiM28v7084Y888JJ8q8j8Djfdj2f0; x-user-id-creator.xiaohongshu.com=618f62ac000000001000d08b; customerClientId=135290779466254; x-user-id-zhaoshang.xiaohongshu.com=6676ec440000000007006f9e; x-user-id-pro.xiaohongshu.com=618f62ac000000001000d08b; x-user-id-ad.xiaohongshu.com=59ebefa3e8ac2b2171a39d89; web_session=040069b0fb21e41280dd4eb3053b4bd730c1f1; x-user-id-ark.xiaohongshu.com=6676ec440000000007006f9e; access-token-ark.xiaohongshu.com=customer.ark.AT-68c5175828067488533217313tlj0kh8ud9qb6m5; x-user-id-school.xiaohongshu.com=6676ec440000000007006f9e; webBuild=5.0.7; unread={%22ub%22:%22693fa572000000001d03b556%22%2C%22ue%22:%2269402eb6000000001d03b9ee%22%2C%22uc%22:40}; acw_tc=0a42136b17659527690507057e5e6da5893a97ce48108c3c2e77f4cd0ac879; websectiga=9730ffafd96f2d09dc024760e253af6ab1feb0002827740b95a255ddf6847fc8; sec_poison_id=c23e710b-6131-4c60-b6b5-6bfbec375bd4; xsecappid=ratlin; customer-sso-sid=68c517584710123740135430ct65fbwbsqmfb0ao; x-user-id-pgy.xiaohongshu.com=62bd9327000000001b0257a1; solar.beaker.session.id=AT-68c517584710123739971599dwhjxeeatgnlqo2c; access-token-pgy.xiaohongshu.com=customer.pgy.AT-68c517584710123739971599dwhjxeeatgnlqo2c; access-token-pgy.beta.xiaohongshu.com=customer.pgy.AT-68c517584710123739971599dwhjxeeatgnlqo2c; loadts=1765953018032',
    }

    json_data = {
        "pageNum": 1,
        "pageSize": 20,
        "sorts": [
            {
                "sort": "",
                "column": "",
            },
        ],
        "heatStartTimeBegin": "2025-11-04",
        "heatStartTimeEnd": "2025-12-17",
        "noteTitle": "",
    }
    url = "https://pgy.xiaohongshu.com/api/solar/heat/data/report"

    rows = fetch_all_heat_reports(
        url=url,
        cookies=cookies,
        headers=headers,
        base_payload=json_data,
        page_size=50,  # 建议调大一点，减少请求次数
        sleep_sec=0.2,
        timeout=20,
        max_retries=3,
    )

    save_json("xhs_heat_report_all.json", rows)

    save_csv("xhs_heat_report_all.csv", rows)

    print(f"\n🎉 完成：共保存 {len(rows)} 条")
