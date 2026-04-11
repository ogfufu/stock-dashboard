import os
import io
import sqlite3
import threading
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/91.0.4472.124 Safari/537.36'
    )
}

DB_PATH = os.path.join(os.path.dirname(__file__), 'history.db')
KEEP_DAYS = 5

# ---------- SQLite history ----------

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute('''
        CREATE TABLE IF NOT EXISTS snapshots (
            date       TEXT NOT NULL,
            rank       INTEGER,
            code       TEXT,
            name       TEXT,
            price      REAL,
            change_pct REAL,
            trust      INTEGER,
            yoy        REAL,
            yoy1       REAL,
            open       REAL,
            high       REAL,
            low        REAL,
            capital    REAL,
            industry   TEXT,
            PRIMARY KEY (date, code)
        )
    ''')
    con.commit()
    con.close()


def save_snapshot(df, date_str):
    """Save today's data; skip if already saved for this date."""
    con = sqlite3.connect(DB_PATH)
    exists = con.execute(
        'SELECT 1 FROM snapshots WHERE date=? LIMIT 1', (date_str,)
    ).fetchone()
    if exists:
        con.close()
        return

    rows = [
        (
            date_str,
            int(r['排序']),
            str(r['代號']),
            r['名稱'],
            r['股價'],
            r['漲跌幅'],
            int(r['投信']) if r['投信'] is not None else None,
            r['月(YOY)'],
            r['月-1(YOY)'],
            r['開盤'],
            r['最高'],
            r['最低'],
            r['資金(億)'],
            r['產業類型'],
        )
        for _, r in df.iterrows()
    ]
    con.executemany(
        'INSERT OR REPLACE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        rows
    )

    # Keep only last KEEP_DAYS dates
    dates = [
        d[0] for d in con.execute(
            'SELECT DISTINCT date FROM snapshots ORDER BY date DESC'
        ).fetchall()
    ]
    if len(dates) > KEEP_DAYS:
        for old in dates[KEEP_DAYS:]:
            con.execute('DELETE FROM snapshots WHERE date=?', (old,))

    con.commit()
    con.close()


def get_history_dates():
    con = sqlite3.connect(DB_PATH)
    dates = [
        d[0] for d in con.execute(
            'SELECT DISTINCT date FROM snapshots ORDER BY date DESC'
        ).fetchall()
    ]
    con.close()
    return dates


def get_snapshot(date_str):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        '''SELECT rank,code,name,price,change_pct,trust,yoy,yoy1,
                  open,high,low,capital,industry
           FROM snapshots WHERE date=? ORDER BY rank''',
        (date_str,)
    ).fetchall()
    con.close()
    cols = ['排序','代號','名稱','股價','漲跌幅','投信','月(YOY)','月-1(YOY)','開盤','最高','最低','資金(億)','產業類型']
    return [dict(zip(cols, r)) for r in rows]


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

    processed['漲跌幅'] = pd.to_numeric(
        processed['漲跌幅'].astype(str)
            .str.replace('+', '', regex=False)
            .str.replace('%', '', regex=False)
            .str.strip(),
        errors='coerce'
    ).fillna(0)

    processed['投信'] = pd.to_numeric(
        processed['投信'], errors='coerce'
    ).fillna(0).round().astype(int)

    numeric_cols = ['股價', '開盤', '最高', '最低', '月(YOY)', '月-1(YOY)', '資金(億)']
    for col in numeric_cols:
        processed[col] = pd.to_numeric(processed[col], errors='coerce')

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
        today = datetime.now().strftime('%Y-%m-%d')
        save_snapshot(df, today)

        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        return jsonify({
            'success': True,
            'data': records,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'count': len(records),
            'date': today,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/history')
def api_history():
    """Return available dates, or snapshot for a specific date."""
    date = request.args.get('date')
    if date:
        records = get_snapshot(date)
        if not records:
            return jsonify({'success': False, 'error': '查無此日期資料'}), 404
        return jsonify({'success': True, 'data': records, 'date': date, 'count': len(records)})
    else:
        dates = get_history_dates()
        return jsonify({'success': True, 'dates': dates})


init_db()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
