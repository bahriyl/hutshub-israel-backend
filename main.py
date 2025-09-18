# app.py
from flask import Flask, request, jsonify, abort
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
import os, re
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import List, Set

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
# читаємо правильний ключ із .env (MONGODB_DB), але підтримуємо і старий MONGO_DB як fallback
DB_NAME = os.getenv("MONGODB_DB") or os.getenv("MONGO_DB")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI is not set (check your .env)")
if not DB_NAME:
    raise RuntimeError("MONGODB_DB (or MONGO_DB) is not set (check your .env)")

app = Flask(__name__)
CORS(app)
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
col = db["properties"]
bookings = db["bookings"]

# ---------- i18n helpers ----------
SUPPORTED_LANGS = {"en", "he"}
DEFAULT_LANG = "en"


def pick_lang_value(value, lang: str):
    """
    value може бути:
      - dict з перекладами: {"en": "...", "he": "..."} або {"en": [...], "he": [...]}
      - просте значення: "..." / [...]/ None
    """
    if isinstance(value, dict):
        if lang in value and value[lang]:
            return value[lang]
        if DEFAULT_LANG in value and value[DEFAULT_LANG]:
            return value[DEFAULT_LANG]
        for v in value.values():
            if v:
                return v
        return None
    return value


def _resolve_lang(req) -> str:
    lang = (req.args.get("lang", "") or "").strip().lower()
    if not lang:
        al = req.headers.get("Accept-Language", "")
        lang = "he" if al.lower().startswith("he") else DEFAULT_LANG
    return lang if lang in SUPPORTED_LANGS else DEFAULT_LANG


def _to_object_id(id_str: str):
    if ObjectId.is_valid(id_str):
        return ObjectId(id_str)
    abort(400, description="Invalid property id")


def parse_date_safe(s: str):
    """ Парсить YYYY-MM-DD -> datetime (UTC start of day). Повертає None при невдачі """
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)  # accepts 'YYYY-MM-DD' -> datetime.date -> treated as 00:00
    except Exception:
        # остання міра: manual parse
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return None


def get_booked_property_ids(start_dt: datetime, end_dt: datetime, allowed_statuses: List[str] = None) -> List[ObjectId]:
    """
    Повертає property_id які мають броні, що перекривають інтервал [start_dt, end_dt].
    Логіка перекриття: booking.start <= end_dt AND booking.end >= start_dt
    """
    if start_dt is None or end_dt is None:
        return []

    q = {
        "start": {"$lte": end_dt},
        "end": {"$gte": start_dt},
    }
    if allowed_statuses:
        q["status"] = {"$in": allowed_statuses}

    # знаходимо distinct property_id
    booked = bookings.distinct("property_id", q)
    # booked може містити ObjectId або рядки — повертаємо список ObjectId
    return [ObjectId(b) if not isinstance(b, ObjectId) else b for b in booked]


# ---------- localization (list vs detail) ----------
def localize_list_doc(doc: dict, lang: str):
    """
    Полегшена версія для списку (карточки):
      - плоскі title/location/amenities
      - одне зображення (doc.image або перше з images)
      - базові числа
    """
    image = doc.get("image")
    if not image:
        imgs = doc.get("images") or []
        if isinstance(imgs, list) and imgs:
            image = imgs[0]

    return {
        "_id": str(doc.get("_id")),
        "title": pick_lang_value(doc.get("title"), lang),
        "location": pick_lang_value(doc.get("location"), lang),
        "region": pick_lang_value(doc.get("region"), lang),
        "amenities": pick_lang_value(doc.get("amenities"), lang) or [],
        "price": doc.get("price"),
        "rating": doc.get("rating"),
        "reviewCount": doc.get("reviewCount"),
        "image": image,
        "isNew": doc.get("isNew", False),
    }


def localize_detail_doc(doc: dict, lang: str):
    """
    Повна версія для деталки:
      - як у списку + всі розширені поля:
        images[], description, maxGuests, minNights,
        host{name, avatar, rating, responseTime, isVerified},
        reviews[{user, rating, date, comment}]
    """
    base = localize_list_doc(doc, lang)
    base.update({
        "images": doc.get("images") or ([base["image"]] if base.get("image") else []),
        "description": pick_lang_value(doc.get("description"), lang),
        "maxGuests": doc.get("maxGuests"),
        "minNights": doc.get("minNights"),
    })

    host = doc.get("host") or {}
    if host:
        base["host"] = {
            "name": pick_lang_value(host.get("name"), lang),
            "avatar": host.get("avatar"),
            "rating": host.get("rating"),
            "responseTime": pick_lang_value(host.get("responseTime"), lang),
            "isVerified": host.get("isVerified", False),
        }

    reviews = doc.get("reviews") or []
    out_reviews = []
    for r in reviews:
        out_reviews.append({
            "user": pick_lang_value(r.get("user"), lang),
            "rating": r.get("rating"),
            "date": r.get("date"),
            "comment": pick_lang_value(r.get("comment"), lang),
        })
    if out_reviews:
        base["reviews"] = out_reviews

    return base


# ---------- API ----------
@app.get("/api/properties")
def list_properties():
    """
    Extended list_properties with filters:
      - q, lang, start, end, guests, minPrice, maxPrice, amenities (comma), region
      - category=<id> OR categories=<id,id,...>, catMode=any|all (default any)
        ids: romantic, family, dogs, luxury, view, jacuzzi
    """
    lang = _resolve_lang(request)

    q_raw = (request.args.get("q", "") or "").strip()
    limit = min(max(int(request.args.get("limit", 30)), 1), 100)
    offset = max(int(request.args.get("offset", 0)), 0)
    sort_param = request.args.get("sort", "new")

    # new filters
    start = (request.args.get("start") or "").strip() or None
    end = (request.args.get("end") or "").strip() or None
    guests = request.args.get("guests")
    min_price = request.args.get("minPrice")
    max_price = request.args.get("maxPrice")
    amenities_raw = request.args.get("amenities")  # "Jacuzzi,Wi-Fi"
    region = (request.args.get("region") or "").strip() or None

    # --- NEW: categories ---
    category_single = (request.args.get("category") or "").strip()
    categories_raw = (request.args.get("categories") or "").strip()
    cat_mode = (request.args.get("catMode") or "any").lower()
    luxury_min_price = float(request.args.get("luxuryMinPrice", 700))

    def _cat_filter(cat_id: str):
        c = (cat_id or "").strip().lower()
        if not c:
            return None
        # amenity helpers (case-insensitive, EN/HE)
        def rx_en(pattern: str):
            return {"$regex": pattern, "$options": "i"}
        def rx_he(pattern: str):
            return {"$regex": pattern, "$options": "i"}

        if c == "jacuzzi":
            return {"$or": [
                {"amenities.en": rx_en(r"jacuzz?i")},
                {"amenities.he": rx_he(r"ג.?קוזי")},
            ]}
        if c == "view":
            return {"$or": [
                {"amenities.en": rx_en(r"(view|mountain|sea\s*view|scenic)")},
                {"amenities.he": rx_he(r"נוף")},
            ]}
        if c == "dogs":
            return {"$or": [
                {"amenities.en": rx_en(r"(dog|pet)")},
                {"amenities.he": rx_he(r"(חיות|כלב)")},
            ]}
        if c == "family":
            return {"maxGuests": {"$gte": 4}}
        if c == "romantic":
            return {"maxGuests": {"$lte": 2}}
        if c == "luxury":
            return {"$or": [
                {"price": {"$gte": luxury_min_price}},
                {"$or": [
                    {"amenities.en": rx_en(r"(jacuzz?i|pool|spa)")},
                    {"amenities.he": rx_he(r"(ג.?קוזי|בריכה|ספא)")},
                ]},
            ]}
        return None
    # --- /NEW ---

    # parse dates
    start_dt = parse_date_safe(start) if start else None
    end_dt = parse_date_safe(end) if end else None
    if start_dt and end_dt and end_dt < start_dt:
        abort(400, description="end date must be >= start date")

    # Base query — text search (title/location/region)
    query = {}
    if q_raw:
        rx = {"$regex": re.escape(q_raw), "$options": "i"}
        query["$or"] = [
            {"title.en": rx}, {"title.he": rx},
            {"location.en": rx}, {"location.he": rx},
            {"region.en": rx}, {"region.he": rx},
        ]

    # region filter
    if region:
        rrx = {"$regex": re.escape(region), "$options": "i"}
        query["$and"] = query.get("$and", []) + [{"$or": [{"region.en": rrx}, {"region.he": rrx}]}]

    # guests filter
    if guests:
        try:
            g = int(guests)
            query["maxGuests"] = {"$gte": g}
        except ValueError:
            abort(400, description="invalid guests value")

    # price filter
    price_q = {}
    if min_price:
        try:
            price_q["$gte"] = float(min_price)
        except ValueError:
            abort(400, description="invalid minPrice")
    if max_price:
        try:
            price_q["$lte"] = float(max_price)
        except ValueError:
            abort(400, description="invalid maxPrice")
    if price_q:
        query["price"] = price_q

    # amenities filter
    if amenities_raw:
        amen_list = [a.strip() for a in amenities_raw.split(",") if a.strip()]
        if amen_list:
            am_q = []
            for a in amen_list:
                rx = {"$regex": re.escape(a), "$options": "i"}
                am_q.append({"amenities.en": rx})
                am_q.append({"amenities.he": rx})
            query["$and"] = query.get("$and", []) + [{"$or": am_q}]

    # --- NEW: apply category filters ---
    cat_ids = []
    if category_single:
        cat_ids.append(category_single)
    if categories_raw:
        cat_ids.extend([c for c in categories_raw.split(",") if c.strip()])

    cat_filters = [f for f in (_cat_filter(c) for c in cat_ids) if f]
    if cat_filters:
        if cat_mode == "all":
            # require ALL selected categories
            query["$and"] = query.get("$and", []) + cat_filters
        else:
            # default: match ANY selected category
            query["$and"] = query.get("$and", []) + [{"$or": cat_filters}]
    # --- /NEW ---

    # availability filter by dates
    if start_dt and end_dt:
        booked_ids = get_booked_property_ids(start_dt, end_dt, allowed_statuses=["confirmed", "paid", None])
        if booked_ids:
            query["_id"] = {"$nin": booked_ids}

    # sorting
    sort_spec = [("_id", DESCENDING)]
    if sort_param == "price_asc":
        sort_spec = [("price", ASCENDING), ("_id", DESCENDING)]
    elif sort_param == "price_desc":
        sort_spec = [("price", DESCENDING), ("_id", DESCENDING)]

    projection = {
        "title": 1, "location": 1, "region": 1, "amenities": 1,
        "price": 1, "rating": 1, "reviewCount": 1, "image": 1, "images": 1, "isNew": 1, "maxGuests": 1
    }

    total = col.count_documents(query)
    cursor = (col.find(query, projection=projection)
              .sort(sort_spec)
              .skip(offset)
              .limit(limit))
    items = [localize_list_doc(doc, lang) for doc in cursor]
    return jsonify({"items": items, "total": total, "lang": lang})


@app.get("/api/properties/<prop_id>")
def get_property(prop_id: str):
    """
    Детальна картка з локалізацією вкладених полів.
    Query:
      - lang: 'en' | 'he' (опційно)
    """
    lang = _resolve_lang(request)
    oid = _to_object_id(prop_id)

    doc = col.find_one({"_id": oid})
    if not doc:
        abort(404, description="Property not found")

    return jsonify(localize_detail_doc(doc, lang))


# ---------- one-time index helper ----------
@app.get("/api/properties/_ensure_indexes")
def ensure_indexes():
    """
    Раз створити індекси для швидкого пошуку. Виклич один раз.
    """
    # Текстовий індекс на мультимовні поля (якщо користуєшся $text)
    col.create_index(
        [("title.en", "text"), ("title.he", "text"),
         ("location.en", "text"), ("location.he", "text"),
         ("region.en", "text"), ("region.he", "text"),
         ("description.en", "text"), ("description.he", "text")],
        name="text_multilang"
    )
    # Для сортування/фільтрів
    col.create_index([("price", ASCENDING)], name="price_asc")
    col.create_index([("_id", DESCENDING)], name="_id_desc")
    return {"ok": True}


if __name__ == '__main__':
    # у проді краще використовувати gunicorn/uvicorn; debug опційно
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", "5000")))
