import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import matplotlib.dates as mdates
from textwrap import dedent


# получение доступа к боту
my_token = ''
bot = telegram.Bot(token=my_token)


# подключение к Clickhouse
connection = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': '',
    'user': 'student',
    'database': 'simulator_20261020'
}


# параметры DAG
default_args = {
    'owner': 'a.egorova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 11, 20),
}

# Интервал запуска DAG
schedule_interval = '0 08 * * *'


# создание DAG с автоматической отправки ежедневного отчета в телеграм
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def daily_report_egorova():
    
    @task()
    def send_daily_report():
        # получаем вчерашнюю дату из контекста Airflow
        context = get_current_context()
        event_date = (context['data_interval_end'].date() - timedelta(days=1)).isoformat()
        
        # получение необходимых данных из ленты новостей
        q = """
        SELECT
            toDate(time) AS date,
            COUNT(DISTINCT user_id) AS active_users,
            SUM(action='like') AS likes,
            SUM(action='view') AS views,
            ROUND(COUNTIf(action = 'like') / nullIf(countIf(action = 'view'), 0) * 100, 2) AS ctr
        FROM {db}.feed_actions
        WHERE toDate(time) BETWEEN toDate('{event_date}') - 6 AND toDate('{event_date}')
        GROUP BY toDate(time)
        """.format(event_date=event_date, db=connection['database'])
        
        feed_tab = ph.read_clickhouse(q, connection=connection)
    
        # сборка отчета по ленте новостей
        context = get_current_context()
        report_date = context['data_interval_end'].date() - timedelta(days=1)
        prev_date = report_date - timedelta(days=1)
        
        feed_tab['date'] = pd.to_datetime(feed_tab['date']).dt.date
        
        yesterday_metrics = feed_tab.loc[feed_tab['date'] == report_date].iloc[0]
        prev_day_metrics = feed_tab.loc[feed_tab['date'] == prev_date].iloc[0]
        
        dau_yesterday = yesterday_metrics['active_users']
        views_yesterday = yesterday_metrics['views']
        likes_yesterday = yesterday_metrics['likes']
        ctr_yesterday =  yesterday_metrics['ctr']
                                                              
        dau_prev = prev_day_metrics['active_users']
        views_prev = prev_day_metrics['views']
        likes_prev = prev_day_metrics['likes']
        ctr_prev = prev_day_metrics['ctr']

        def calc_pct_diff(curr, prev):
            return (curr - prev) / prev * 100 if prev != 0 else 0.0
        
        report_text = dedent(f"""
            📊 <b>Ежедневный отчёт — Лента новостей</b>
            📅 Дата: {report_date}
            <b>Ключевые метрики за вчера:</b>
            👥 DAU: {dau_yesterday:,} ({calc_pct_diff(dau_yesterday, dau_prev):+.2f}%)
            👁 Просмотры: {views_yesterday:,} ({calc_pct_diff(views_yesterday, views_prev):+.2f}%)
            ❤️ Лайки: {likes_yesterday:,} ({calc_pct_diff(likes_yesterday, likes_prev):+.2f}%)
            📈 CTR: {ctr_yesterday:.2f}% ({(ctr_yesterday - ctr_prev):+.2f} п.п.)
        """).strip()
        
        # график DAU
        fig, ax = plt.subplots(figsize=(12, 5))

        sns.lineplot(data=feed_tab, x='date', y='active_users', marker='o', linewidth=2, ax=ax)
        
        ax.set_title('Дневная активная аудитория', fontsize=15, pad=15)
        ax.set_xlabel('Дата')
        ax.set_ylabel('Количество пользователей')

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        ax.grid(axis='y', alpha=0.3)
        ax.grid(axis='x', visible=False)
        sns.despine()

        fig.tight_layout()

        dau_plot = io.BytesIO()
        fig.savefig(dau_plot, format='png', dpi=150, bbox_inches='tight')
        dau_plot.seek(0)
        dau_plot.name = 'dau_plot.png'

        plt.close(fig)
        
        # график лайков и просмотров
        fig, ax = plt.subplots(figsize=(12, 5))

        sns.lineplot(data=feed_tab, x='date', y='likes', marker='o', linewidth=2, label='Лайки', ax=ax)
        sns.lineplot(data=feed_tab, x='date', y='views', marker='o', linewidth=2, label='Просмотры', ax=ax)

        ax.set_title('Просмотры и лайки', fontsize=15, pad=15)
        ax.set_xlabel('Дата')
        ax.set_ylabel('Количество событий')

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        ax.legend(title='Метрика', frameon=False)
        ax.grid(axis='y', alpha=0.3)
        ax.grid(axis='x', visible=False)
        sns.despine()

        fig.tight_layout()

        likes_views_plot = io.BytesIO()
        fig.savefig(likes_views_plot, format='png', dpi=150, bbox_inches='tight')
        likes_views_plot.seek(0)
        likes_views_plot.name = 'likes_views_plot.png'

        plt.close(fig)
        
        # график CTR
        fig, ax = plt.subplots(figsize=(12, 5))

        sns.lineplot(data=feed_tab, x='date', y='ctr', marker='o', linewidth=2, ax=ax)

        ax.set_title('CTR ленты новостей', fontsize=15, pad=15)
        ax.set_xlabel('Дата')
        ax.set_ylabel('CTR, %')

        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        ax.grid(axis='y', alpha=0.3)
        ax.grid(axis='x', visible=False)
        sns.despine()

        fig.tight_layout()

        ctr_plot = io.BytesIO()
        fig.savefig(ctr_plot, format='png', dpi=150, bbox_inches='tight')
        ctr_plot.seek(0)
        ctr_plot.name = 'ctr_plot.png'

        plt.close(fig)
    
        # отправка отчета в телеграм
        chat_id = 336718279
        
        msg = report_text
        bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='HTML')
        
        bot.sendPhoto(chat_id=chat_id, photo=dau_plot)
        bot.sendPhoto(chat_id=chat_id, photo=likes_views_plot)
        bot.sendPhoto(chat_id=chat_id, photo=ctr_plot)
        
    send_daily_report()
            
daily_report_egorova = daily_report_egorova()