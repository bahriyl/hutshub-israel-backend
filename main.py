# app.py
from flask import Flask, request, jsonify, abort
from flask_cors import CORS
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
import os, re
from dotenv import load_dotenv
from datetime import datetime, timezone
from typing import List, Set
import requests
from copy import deepcopy

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
# читаємо правильний ключ із .env (MONGODB_DB), але підтримуємо і старий MONGO_DB як fallback
DB_NAME = os.getenv("MONGO_DB")

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

EARTH_RADIUS_M = 6378137.0  # meters


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
        "cancellationPolicy": pick_lang_value(doc.get("cancellationPolicy"), lang),
        "smokingPolicy": pick_lang_value(doc.get("smokingPolicy"), lang),
        "checkinTime": doc.get("checkinTime"),
        "checkoutTime": doc.get("checkoutTime"),
        "cleaningFee": doc.get("cleaningFee"),
        "serviceFee": doc.get("serviceFee")
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
      - near=lat,lon & radiusKm=NN
      - sort=new|price_asc|price_desc
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

    # --- categories ---
    category_single = (request.args.get("category") or "").strip()
    categories_raw = (request.args.get("categories") or "").strip()
    cat_mode = (request.args.get("catMode") or "any").lower()
    luxury_min_price = float(request.args.get("luxuryMinPrice", 700))

    def _cat_filter(cat_id: str):
        c = (cat_id or "").strip().lower()
        if not c:
            return None

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

    # parse dates
    start_dt = parse_date_safe(start) if start else None
    end_dt = parse_date_safe(end) if end else None
    if start_dt and end_dt and end_dt < start_dt:
        abort(400, description="end date must be >= start date")

    # Base query — text search (title/location/region)
    query: dict = {}
    if q_raw:
        rx = {"$regex": re.escape(q_raw), "$options": "i"}
        # backward-compat: old shape (strings/objects) + new structured displayName
        query["$or"] = [
            {"title.en": rx}, {"title.he": rx},  # old localized titles
            {"title": rx},  # if title is a plain string
            {"location.en": rx}, {"location.he": rx},  # legacy location field
            {"region.en": rx}, {"region.he": rx},      # legacy region field
            {"location.displayName.en": rx}, {"location.displayName.he": rx},  # new structured field
            {"location.displayName": rx},  # if stored as plain string
        ]

    # region filter (legacy support)
    if region:
        rrx = {"$regex": re.escape(region), "$options": "i"}
        query["$and"] = query.get("$and", []) + [{
            "$or": [
                {"region.en": rrx}, {"region.he": rrx},
                {"location.displayName.en": rrx}, {"location.displayName.he": rrx},
                {"location.displayName": rrx},
            ]
        }]

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

    # geospatial filter
    use_near = False
    coords_for_within = None
    max_m = None

    near = request.args.get("near")  # "lat,lon"
    radius_km = float(request.args.get("radiusKm", 0) or 0)
    if near and radius_km > 0:
        try:
            lat, lon = map(float, near.split(","))
            max_m = int(radius_km * 1000)
            coords_for_within = [lon, lat]  # GeoJSON order
            query["location.geo"] = {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": coords_for_within},
                    "$maxDistance": max_m
                }
            }
            use_near = True
        except Exception:
            # ignore malformed near
            pass

    # apply category filters
    cat_ids = []
    if category_single:
        cat_ids.append(category_single)
    if categories_raw:
        cat_ids.extend([c for c in categories_raw.split(",") if c.strip()])

    cat_filters = [f for f in (_cat_filter(c) for c in cat_ids) if f]
    if cat_filters:
        if cat_mode == "all":
            query["$and"] = query.get("$and", []) + cat_filters
        else:
            query["$and"] = query.get("$and", []) + [{"$or": cat_filters}]

    # availability filter by dates
    if start_dt and end_dt:
        booked_ids = get_booked_property_ids(start_dt, end_dt, allowed_statuses=["confirmed", "paid", None])
        if booked_ids:
            query["_id"] = {"$nin": booked_ids}

    # sorting (note: when using $near, don't set an explicit sort)
    sort_spec = [("_id", DESCENDING)]
    if sort_param == "price_asc":
        sort_spec = [("price", ASCENDING), ("_id", DESCENDING)]
    elif sort_param == "price_desc":
        sort_spec = [("price", DESCENDING), ("_id", DESCENDING)]

    # If user asked for price sort + radius, prefer price order inside circle:
    if use_near and sort_param in ("price_asc", "price_desc") and coords_for_within and max_m is not None:
        query["location.geo"] = {
            "$geoWithin": {
                "$centerSphere": [coords_for_within, float(max_m) / EARTH_RADIUS_M]
            }
        }
        use_near = False  # allow sort below

    projection = {
        "title": 1, "location": 1, "region": 1, "amenities": 1,
        "price": 1, "rating": 1, "reviewCount": 1, "image": 1, "images": 1, "isNew": 1, "maxGuests": 1
    }

    # -------- count-safe query (swap $near -> $geoWithin) --------
    query_for_count = deepcopy(query)
    geoQ = query_for_count.get("location.geo")
    if isinstance(geoQ, dict) and "$near" in geoQ:
        near_obj = geoQ["$near"]
        coords = near_obj.get("$geometry", {}).get("coordinates")
        max_m2 = near_obj.get("$maxDistance")
        if coords and isinstance(max_m2, (int, float)):
            query_for_count["location.geo"] = {
                "$geoWithin": {
                    "$centerSphere": [coords, float(max_m2) / EARTH_RADIUS_M]
                }
            }
        else:
            query_for_count.pop("location.geo", None)
    # ------------------------------------------------------------

    # totals
    total = col.count_documents(query_for_count)

    # items
    cursor = col.find(query, projection=projection)
    if not use_near and sort_spec:
        cursor = cursor.sort(sort_spec)  # don't sort when using $near (distance order)
    cursor = cursor.skip(offset).limit(limit)

    items = [localize_list_doc(doc, lang) for doc in cursor]
    return jsonify({"items": items, "total": total, "lang": lang})


# GET /api/autocomplete/properties?q=&lang=en&limit=7
@app.get("/api/autocomplete/properties")
def props_autocomplete():
    lang = _resolve_lang(request)
    q = (request.args.get("q") or "").strip()
    limit = min(max(int(request.args.get("limit", 7)), 1), 20)
    if not q:
        return jsonify([])

    rx = {"$regex": re.escape(q), "$options": "i"}
    # match in title (en/he)
    cur = col.find(
        {"$or": [{"title.en": rx}, {"title.he": rx}]},
        projection={"_id": 1, "title": 1, "image": 1}
    ).limit(limit)
    items = [{"_id": str(d["_id"]), "title": d.get("title"), "image": d.get("image")} for d in cur]
    return jsonify(items)


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


@app.get("/api/places/autocomplete")
def places_autocomplete():
    """
    GeoNames-backed autocomplete for 'Where' picker.
    Returns an array of suggestions with lat/lon and a display string.

    Query params:
      q        : user input (required)
      limit    : max results (default 8, max 15)
      lang     : 'en' or 'he' (default 'en')
      country  : ISO2 country code (default 'IL')
      # optional bias:
      near     : "lat,lon" -> we use GeoNames 'orderby=distance' when provided
    """
    q = (request.args.get("q") or "").strip()
    if not q:
        return jsonify([])

    # basic args
    limit = max(1, min(int(request.args.get("limit", 8)), 15))
    lang = (request.args.get("lang") or "en").lower()
    if lang not in ("en", "he"):
        lang = "en"
    country = (request.args.get("country") or "IL").upper()
    near = (request.args.get("near") or "").strip()

    # ---- simple cache (24h) to keep within free GeoNames limits ----
    cache_key = {
        "q": q,
        "limit": limit,
        "lang": lang,
        "country": country,
        "near": near,
    }
    cached = db.places_cache.find_one(cache_key)
    if cached:
        # TTL index will expire old docs, but double-check 24h freshness:
        if (datetime.utcnow() - cached["ts"]).total_seconds() < 86400:
            return jsonify(cached["data"])

    username = os.getenv("GEONAMES_USER")
    if not username:
        abort(500, description="GEONAMES_USER env var is not set")

    # Build GeoNames request
    # We're using the /searchJSON endpoint with:
    # - name_startsWith: for autocomplete
    # - featureClass=P : populated places (cities/towns)
    # - country: restrict to IL by default (your use case)
    # - orderby=population or distance (if near given)
    params = {
        "name_startsWith": q,
        "maxRows": limit,
        "featureClass": "P",
        "country": country,
        "lang": "en" if lang == "he" else "en",  # GeoNames "he" coverage is spotty; we'll localize display ourselves
        "username": username,
    }
    # if we have a 'near' bias, switch ordering to distance using 'lat/lng'
    if near:
        try:
            lat_str, lon_str = near.split(",")
            lat, lon = float(lat_str), float(lon_str)
            params["lat"] = lat
            params["lng"] = lon
            params["orderby"] = "distance"
        except Exception:
            pass
    else:
        params["orderby"] = "population"

    try:
        r = requests.get("https://secure.geonames.org/searchJSON", params=params, timeout=8)
        r.raise_for_status()
        raw = r.json()
        hits = raw.get("geonames", []) or []
    except Exception as e:
        # In case GeoNames is down, fail gracefully
        return jsonify([])

    # Transform to your frontend shape
    # For Israel-only you can keep just displayName+geo,
    # but we also include 'name/admin1/country' to help future UI tweaks.
    out = []
    for h in hits:
        try:
            lat = float(h["lat"])
            lon = float(h["lng"])
        except Exception:
            continue

        name = h.get("name") or ""
        admin1 = h.get("adminName1") or ""
        country_name = h.get("countryName") or ""

        # Basic display in current UI language (you can later pass through your i18n if needed)
        if lang == "he":
            display = ", ".join([x for x in [name, admin1] if x])  # short Hebrew line
        else:
            display = ", ".join([x for x in [name, admin1, country_name] if x])

        out.append({
            "lat": lat,
            "lon": lon,
            "name": name,
            "admin1": admin1,
            "country": country_name,
            "countryCode": h.get("countryCode"),
            "geonameId": h.get("geonameId"),
            "display": display,
        })

    # store in cache
    db.places_cache.update_one(
        cache_key,
        {"$set": {"ts": datetime.utcnow(), "data": out}},
        upsert=True
    )

    return jsonify(out)


if __name__ == '__main__':
    # у проді краще використовувати gunicorn/uvicorn; debug опційно
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", "5000")))
