import csv
import time
import json
import requests

API_KEY      = "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"
API_URL      = "https://api.groq.com/openai/v1/chat/completions"
MODEL        = "meta-llama/llama-4-scout-17b-16e-instruct"
CSV_PATH     = "bbc-news-data.csv"   
OUTPUT_FILE  = "summaries.json"      
MAX_ARTICLES = 10                    
DELAY_SEC    = 1.5                   

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


def read_news_csv(path: str) -> list:
    """Читает CSV-файл с новостями."""
    articles = []
    with open(path, newline="", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        delimiter = "\t" if "\t" in sample else ","
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            articles.append(row)
    return articles


def summarize_article(title: str, content: str) -> str:
    """Отправляет статью и возвращает краткое содержание."""
    prompt = (
        f"Дай краткое содержание следующей новости на русском языке "
        f"в 2–3 предложениях.\n\n"
        f"Заголовок: {title}\n\n"
        f"Текст: {content[:3000]}"
    )

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "system",
                "content": "Ты — новостной редактор. Пиши чётко и лаконично."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 300,
        "temperature": 0.3,
    }

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"].strip()

    except requests.exceptions.HTTPError as e:
        return f"[Ошибка HTTP {e.response.status_code}: {e.response.text[:200]}]"
    except requests.exceptions.ConnectionError:
        return "[Ошибка: нет подключения к интернету]"
    except requests.exceptions.Timeout:
        return "[Ошибка: превышено время ожидания]"
    except Exception as e:
        return f"[Неизвестная ошибка: {e}]"


def run_pipeline():
    print("=" * 60)
    print("  Пайплайн: Новости - Краткое содержание")
    print("=" * 60)

    # 1. Читаем CSV
    print(f"\n[1/3] Читаем файл: {CSV_PATH}")
    articles = read_news_csv(CSV_PATH)
    print(f"      Найдено статей: {len(articles)}")

    if MAX_ARTICLES is not None:
        articles = articles[:MAX_ARTICLES]
        print(f"      Обрабатываем первые: {MAX_ARTICLES}")

    # 2. Генерируем краткие содержания
    print(f"\n[2/3] Отправляем в ({MODEL})...")
    results = []
    for i, article in enumerate(articles, 1):
        title    = article.get("title", "Без заголовка").strip()
        content  = article.get("content", "").strip()
        category = article.get("category", "unknown").strip()

        print(f"  [{i}/{len(articles)}] {title[:60]}...")
        summary = summarize_article(title, content)
        results.append({
            "id":       i,
            "category": category,
            "title":    title,
            "summary":  summary,
        })

        if i < len(articles):
            time.sleep(DELAY_SEC)

    # 3. Сохраняем результат в JSON
    print(f"\n[3/3] Сохраняем результаты в: {OUTPUT_FILE}")

    output = {
        "meta": {
            "source":        "BBC News Dataset",
            "model":         MODEL,
            "total_articles": len(results),
        },
        "articles": results,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"   Готово! Обработано {len(results)} статей.")
    print(f"   Результат сохранён в: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
