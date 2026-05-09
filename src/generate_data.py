from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from config import RAW_DIR, ensure_directories


RNG = np.random.default_rng(42)
N_USERS = 50_000
N_PRODUCTS = 600
DAYS = 90

REGIONS = ["London", "South East", "Midlands", "North West", "Scotland", "Wales"]
DEVICES = ["mobile", "desktop", "tablet"]
CHANNELS = ["app", "web"]
CUSTOMER_TYPES = ["new", "returning"]
FUNNEL_STEPS = [
    "homepage_view",
    "search",
    "product_view",
    "add_to_basket",
    "checkout_start",
    "payment_complete",
]


def weighted_choice(values, weights, size):
    return RNG.choice(values, size=size, p=np.array(weights) / np.sum(weights))


def generate_users():
    signup_start = datetime.now().date() - timedelta(days=540)
    users = pd.DataFrame(
        {
            "user_id": np.arange(1, N_USERS + 1),
            "region": weighted_choice(REGIONS, [0.30, 0.18, 0.17, 0.15, 0.12, 0.08], N_USERS),
            "customer_type": weighted_choice(CUSTOMER_TYPES, [0.28, 0.72], N_USERS),
            "loyalty_flag": RNG.choice([True, False], size=N_USERS, p=[0.38, 0.62]),
            "signup_date": [
                signup_start + timedelta(days=int(x)) for x in RNG.integers(0, 540, size=N_USERS)
            ],
        }
    )
    return users


def generate_products():
    categories = ["Fresh", "Dairy", "Bakery", "Frozen", "Household", "Snacks", "Drinks", "Health"]
    products = pd.DataFrame(
        {
            "product_id": np.arange(1, N_PRODUCTS + 1),
            "product_name": [f"Product {i:03d}" for i in range(1, N_PRODUCTS + 1)],
            "category": weighted_choice(categories, [0.20, 0.16, 0.12, 0.12, 0.11, 0.13, 0.10, 0.06], N_PRODUCTS),
            "base_price": np.round(RNG.gamma(shape=3.0, scale=2.3, size=N_PRODUCTS) + 0.75, 2),
            "margin_rate": np.round(RNG.uniform(0.12, 0.42, size=N_PRODUCTS), 2),
        }
    )
    return products


def generate_assignments(users):
    return pd.DataFrame(
        {
            "user_id": users["user_id"],
            "experiment_name": "checkout_friction_reduction",
            "variant": RNG.choice(["control", "variant"], size=len(users), p=[0.5, 0.5]),
            "assigned_at": pd.Timestamp(datetime.now().date() - timedelta(days=DAYS)),
        }
    )


def step_probability(step, user, device, channel, variant):
    probs = {
        "search": 0.72,
        "product_view": 0.78,
        "add_to_basket": 0.43,
        "checkout_start": 0.58,
        "payment_complete": 0.63,
    }
    p = probs[step]
    if user.customer_type == "returning":
        p += 0.05
    if user.loyalty_flag:
        p += 0.04
    if device == "mobile" and step == "checkout_start":
        p -= 0.13
    if device == "desktop" and step in {"checkout_start", "payment_complete"}:
        p += 0.04
    if channel == "app" and step in {"add_to_basket", "payment_complete"}:
        p += 0.03
    if variant == "variant" and step == "payment_complete":
        p += 0.035
        if user.customer_type == "returning":
            p += 0.025
        if device == "mobile":
            p -= 0.012
    return float(np.clip(p, 0.08, 0.93))


def generate_sessions_events_orders(users, products, assignments):
    today = datetime.now().date()
    start = datetime.combine(today - timedelta(days=DAYS - 1), datetime.min.time())
    assignment_lookup = assignments.set_index("user_id")["variant"].to_dict()
    user_records = users.set_index("user_id")
    product_ids = products["product_id"].to_numpy()
    product_prices = products.set_index("product_id")["base_price"].to_dict()

    sessions = []
    events = []
    orders = []
    session_id = 1
    event_id = 1
    order_id = 1

    session_counts = RNG.poisson(lam=3.2, size=N_USERS)
    active_user_ids = users.loc[session_counts > 0, "user_id"].to_numpy()

    for user_id, session_count in zip(users["user_id"], session_counts):
        user = user_records.loc[user_id]
        variant = assignment_lookup[user_id]
        for _ in range(int(session_count)):
            session_start = start + timedelta(
                days=int(RNG.integers(0, DAYS)),
                hours=int(RNG.integers(6, 23)),
                minutes=int(RNG.integers(0, 60)),
                seconds=int(RNG.integers(0, 60)),
            )
            device = weighted_choice(DEVICES, [0.62, 0.30, 0.08], 1)[0]
            channel = weighted_choice(CHANNELS, [0.56, 0.44], 1)[0]
            sessions.append(
                {
                    "session_id": session_id,
                    "user_id": user_id,
                    "session_start": session_start,
                    "device_type": device,
                    "channel": channel,
                }
            )

            event_time = session_start
            reached_steps = ["homepage_view"]
            for step in FUNNEL_STEPS[1:]:
                if RNG.random() <= step_probability(step, user, device, channel, variant):
                    reached_steps.append(step)
                else:
                    break

            viewed_product = int(RNG.choice(product_ids))
            basket_product_count = int(np.clip(RNG.poisson(5), 1, 24))
            basket_value = 0.0
            for step in reached_steps:
                event_time += timedelta(seconds=int(RNG.integers(20, 240)))
                events.append(
                    {
                        "event_id": event_id,
                        "session_id": session_id,
                        "user_id": user_id,
                        "event_timestamp": event_time,
                        "event_name": step,
                        "product_id": viewed_product if step in {"product_view", "add_to_basket"} else np.nan,
                        "device_type": device,
                        "region": user.region,
                        "customer_type": user.customer_type,
                        "loyalty_flag": user.loyalty_flag,
                        "channel": channel,
                    }
                )
                event_id += 1

            if reached_steps[-1] == "payment_complete":
                bought_products = RNG.choice(product_ids, size=basket_product_count, replace=True)
                basket_value = float(sum(product_prices[int(pid)] for pid in bought_products))
                basket_value *= float(RNG.uniform(0.92, 1.12))
                delivery_fee = 0 if user.loyalty_flag else float(RNG.choice([0, 2.99, 4.99], p=[0.45, 0.35, 0.20]))
                revenue = round(basket_value + delivery_fee, 2)
                orders.append(
                    {
                        "order_id": order_id,
                        "session_id": session_id,
                        "user_id": user_id,
                        "order_timestamp": event_time,
                        "revenue": revenue,
                        "basket_value": round(basket_value, 2),
                        "items_count": basket_product_count,
                        "device_type": device,
                        "region": user.region,
                        "customer_type": user.customer_type,
                        "loyalty_flag": user.loyalty_flag,
                        "channel": channel,
                        "variant": variant,
                    }
                )
                order_id += 1

            session_id += 1

    print(f"Active users generated: {len(active_user_ids):,}")
    return pd.DataFrame(sessions), pd.DataFrame(events), pd.DataFrame(orders)


def main():
    ensure_directories()
    users = generate_users()
    products = generate_products()
    assignments = generate_assignments(users)
    sessions, events, orders = generate_sessions_events_orders(users, products, assignments)

    users.to_csv(RAW_DIR / "users.csv", index=False)
    products.to_csv(RAW_DIR / "products.csv", index=False)
    assignments.to_csv(RAW_DIR / "experiment_assignments.csv", index=False)
    sessions.to_csv(RAW_DIR / "sessions.csv", index=False)
    events.to_csv(RAW_DIR / "events.csv", index=False)
    orders.to_csv(RAW_DIR / "orders.csv", index=False)

    print("Synthetic e-commerce data generated in data/raw")
    print(f"Users: {len(users):,} | Sessions: {len(sessions):,} | Events: {len(events):,} | Orders: {len(orders):,}")


if __name__ == "__main__":
    main()
