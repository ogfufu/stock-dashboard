import os
import io
import threading
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}

# ---------- In-memory cache (Wespai changes once a day) ----------
_wespai_cache = {'data': None, 'date': None}
_wespai_lock = threading.Lock()


def get_wespai_data():
    with _wespai_lock:
        today = datetime.now().strftime('%Y-%m-%d')
        if _wespai_cache['data'] is not None and _wespai_cache['date'] == today:
            return _wespai_cache['data']

        url = 'https://stock.wespai.com/p/71294'
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        df_all = pd.read_html(io.StringIO(r.text))[0]
        cols = ['代號', '公司', '投信買賣超', '(月)營收年增率(%)', '(月-1)營收年增率(%)', '產業類型']
        df = df_all[cols].copy()
        df['代號'] = df['代號'].astype(str)

        _wespai_cache['data'] = df
        _wespai_cache['date'] = today
        return df


def get_histock_data():
    url = 'https://histock.tw/stock/rank.aspx?m=13&p=all'
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    df_all = pd.read_html(io.StringIO(r.text))[0]
    df_all.columns = df_all.columns.str.replace('▼', '', regex=False)
    cols = ['代號', '名稱', '價格', '漲跌', '漲跌幅', '開盤', '最高', '最低', '昨收', '成交值(億)']
    df = df_all[cols].copy()
    df['代號'] = df['代號'].astype(str)
    return df


def run_stock_update():
    df_hi = get_histock_data()
    df_wes = get_wespai_data()

    merged = pd.merge(df_hi, df_wes, on='代號', how='inner')
    processed = merged.rename(columns={
        '價格': '股價',
        '投信買賣超': '投信',
        '(月)營收年增率(%)': '月(YOY)',
        '(月-1)營收年增率(%)': '月-1(YOY)',
        '成交值(億)': '資金(億)',
    })

    # 漲跌幅 (%) — strip +/% signs then parse
    processed['漲跌幅'] = pd.to_numeric(
        processed['漲跌幅'].astype(str)
            .str.replace('+', '', regex=False)
            .str.replace('%', '', regex=False)
            .str.strip(),
        errors='coerce'
    ).fillna(0)

    # 投信 as integer (no decimal)
    processed['投信'] = pd.to_numeric(
        processed['投信'], errors='coerce'
    ).fillna(0).round().astype(int)

    numeric_cols = ['股價', '開盤', '最高', '最低', '月(YOY)', '月-1(YOY)', '資金(億)']
    for col in numeric_cols:
        processed[col] = pd.to_numeric(processed[col], errors='coerce')

    # Sort by 資金(億) descending, keep top 100
    processed = (
        processed
        .sort_values('資金(億)', ascending=False)
        .head(100)
        .reset_index(drop=True)
    )
    processed.insert(0, '排序', range(1, len(processed) + 1))

    final_cols = ['排序', '代號', '名稱', '股價', '漲跌幅', '投信', '月(YOY)', '月-1(YOY)', '開盤', '最高', '最低', '資金(億)', '產業類型']
    return processed[final_cols]


# ---------- Routes ----------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/stocks')
def api_stocks():
    try:
        df = run_stock_update()
        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        return jsonify({
            'success': True,
            'data': records,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(records),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
