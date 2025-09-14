import pandas as pd
import os
import g4f
from g4f.client import Client
import json
import re

products_description = """
1. Карта для путешествий
кешбэк на поездки/такси.
Сигнал: Путешествия/Отели/Такси, fx_buy/fx_sell
2. Премиальная карта
до 4% кешбэк + бонусы на рестораны/ювелирку/косметику.
Сигнал: высокий баланс, частые atm_withdrawal и p2p_out, активные рестораны/косметика/ювелирка
3. Кредитная карта
до 10% кешбэк, льготный период, рассрочка.
Сигнал: выраженные категории трат, онлайн-сервисы.
4. Обмен валют
экономия на спреде.
Сигнал: fx_buy/fx_sell.
5. Кредит наличными
быстрые деньги, гибкие погашения.
Сигнал: низкий баланс, регулярные loan_payment_out.
6. Депозит мультивалютный
проценты + хранение валют.
Сигнал: Большой баланс, fx_buy, fx_sell, deposit_fx_topup_out, deposit_fx_withdraw_in
7. Депозит сберегательный
максимальная ставка.
Сигнал: крупный стабильный остаток.
8. Депозит накопительный
повышенная ставка, пополнения без снятий.
Сигнал: средний баланс.
9. Инвестиции (брокерский счёт)
низкий порог, сниженные комиссии.
Сигнал: Огромный баланс.
10. Золотые слитки
защитный актив.
Сигнал: gold_buy_out, gold_sell_in.
"""

def build_prompt(clients_data):
    """    clients_data: список словарей с данными клиентов
    """
    prompt = f"""
Ты — рекомендательная система банка.
На вход тебе даются данные о клиентах: их топ-категории расходов, типы транзакций, баланс и другие сигналы. 
Твоя задача: выбрать 1 наиболее подходящий продукт для каждого клиента и составить персональное push-уведомление.
Среднестатистический баланс: 600000
Описание продуктов:
{products_description}
Формат входных данных (JSON):
{json.dumps(clients_data, ensure_ascii=False, indent=2)}
Отвечай строго в формате JSON. Никакого текста вокруг. Сигналы клиента не пиши:
[
  {{
    "client_code": client_code,
    "recomend_product": recomend_product,
    "push_notification": push
  }}
]
Правила написания. Строго соблюдай их:
в recomend_product напиши только полноценное название рекомендуемого продукта.
в push Поздоровайся с именем и предложи ему услугу, используй только 3 коротких предложения. Ты должен заинтерисовать его. 
Обращайся к клиенту исключительно на вы.
В конце добавь глагол действия 1-2 словами в начальной форме. Заканчивать только на точку.
Если статус клиента: Премиальный клиент, то премиальную карту уже не предлагать. Предложи другое, на основе других его интересов, транзакций.
Если возраст клиента младше 30, сделай уведомление более эмоциональным и энергичным.
Если возраст клиента свыше 50, сделай уведомление более серьезным.
Длина push 180-200 символов.
"""
    return prompt

results = []
client = Client()

clients_info = pd.read_csv("db/clients.csv")

batch_size = 10
total_clients = len(clients_info)

for start_idx in range(0, total_clients, batch_size):
    batch_clients = []
    end_idx = min(start_idx + batch_size, total_clients)

    for i in range(start_idx, end_idx):
        client_id = clients_info.loc[i, 'client_code']
        transactions_file = f"db/client_{client_id}_transactions_3m.csv"
        transfers_file = f"db/client_{client_id}_transfers_3m.csv"

        if not os.path.exists(transactions_file) or not os.path.exists(transfers_file):
            print(client_id, "Нет данных. Пропуск")
            batch_clients.append({
                "client_code": int(client_id),
                "name": "Нет данных",
                "status": "Нет данных",
                "top_category": [],
                "top_transfer": [],
                "avg_balance": 0,
                "age": 0
            })
            continue

        transactions = pd.read_csv(transactions_file)
        transfers = pd.read_csv(transfers_file)

        top_categories = transactions['category'].value_counts().head(5).index.tolist()
        top_transfers = transfers['type'].value_counts().head(5).index.tolist()

        batch_clients.append({
            "client_code": int(client_id),
            "name": str(transactions.loc[0, 'name']),
            "status": str(clients_info.loc[i, 'status']),
            "top_category": top_categories,
            "top_transfer": top_transfers,
            "avg_balance": float(clients_info.loc[i, 'avg_monthly_balance_KZT']),
            "age": int(clients_info.loc[i, 'age'])
        })

    content = build_prompt(batch_clients)

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            provider=g4f.Provider.Yqcloud,
            messages=[{"role": "user", "content": content}],
            web_search=False
        )

        raw_answer = response.choices[0].message.content

        cleaned = re.sub(r"^```json\s*|\s*```$", "", raw_answer.strip(), flags=re.DOTALL)
        answers = json.loads(cleaned)

        results.extend(answers)
        print(f"Обработаны клиенты {start_idx + 1} до {end_idx}")
        print(results)

    except Exception as e:
        print("Ошибка при обработке пакета клиентов:", e)
        # Если ошибка — добавим заглушку для всех клиентов в пакете
        for c in batch_clients:
            results.append({
                "client_code": c["client_code"],
                "recomend_product": "Ошибка",
                "push_notification": "Ошибка генерации уведомления"
            })

results_df = pd.DataFrame(results)
results_df.to_csv("push_notifications.csv", index=False, encoding="utf-8-sig")
print("Готово! Результаты сохранены в push_notifications.csv")
