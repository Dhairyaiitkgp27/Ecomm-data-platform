"""
Reference data for the synthetic Indian e-commerce generator.

Everything here is hand-curated so the generated data has *realistic Indian
characteristics*: real cities mapped to their states, population-weighted so
metros dominate (as they do in real Indian e-commerce), an Indian category
taxonomy with realistic price bands in INR, and common Indian first/last names.

None of this is scraped from a private source; it is public-knowledge reference
data used only to make the synthetic dataset believable.
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# Geography: (city, state, delivery_tier). Weight approximates order share.
# delivery_tier drives realistic shipping times downstream:
#   1 = metro / very well connected, 2 = tier-2, 3 = tier-3 / remote
# --------------------------------------------------------------------------- #
CITIES = [
    # city,           state,             tier, weight
    ("Mumbai",        "Maharashtra",       1, 130),
    ("Delhi",         "Delhi",             1, 125),
    ("Bengaluru",     "Karnataka",         1, 120),
    ("Hyderabad",     "Telangana",         1,  85),
    ("Chennai",       "Tamil Nadu",        1,  80),
    ("Kolkata",       "West Bengal",       1,  70),
    ("Pune",          "Maharashtra",       1,  75),
    ("Ahmedabad",     "Gujarat",           1,  55),
    ("Jaipur",        "Rajasthan",         2,  40),
    ("Surat",         "Gujarat",           2,  30),
    ("Lucknow",       "Uttar Pradesh",     2,  32),
    ("Kanpur",        "Uttar Pradesh",     2,  20),
    ("Nagpur",        "Maharashtra",       2,  22),
    ("Indore",        "Madhya Pradesh",    2,  26),
    ("Bhopal",        "Madhya Pradesh",    2,  18),
    ("Patna",         "Bihar",             2,  20),
    ("Chandigarh",    "Chandigarh",        1,  24),
    ("Coimbatore",    "Tamil Nadu",        2,  18),
    ("Kochi",         "Kerala",            2,  22),
    ("Thiruvananthapuram", "Kerala",       2,  14),
    ("Visakhapatnam", "Andhra Pradesh",    2,  18),
    ("Vijayawada",    "Andhra Pradesh",    3,  12),
    ("Guwahati",      "Assam",             3,  14),
    ("Bhubaneswar",   "Odisha",            2,  15),
    ("Ranchi",        "Jharkhand",         3,  11),
    ("Raipur",        "Chhattisgarh",      3,  10),
    ("Dehradun",      "Uttarakhand",       2,  10),
    ("Ludhiana",      "Punjab",            2,  16),
    ("Amritsar",      "Punjab",            2,  12),
    ("Nashik",        "Maharashtra",       2,  14),
    ("Vadodara",      "Gujarat",           2,  16),
    ("Rajkot",        "Gujarat",           2,  12),
    ("Varanasi",      "Uttar Pradesh",     3,  12),
    ("Agra",          "Uttar Pradesh",     3,  11),
    ("Meerut",        "Uttar Pradesh",     3,  10),
    ("Jodhpur",       "Rajasthan",         3,  10),
    ("Madurai",       "Tamil Nadu",        3,  11),
    ("Mysuru",        "Karnataka",         2,  13),
    ("Mangaluru",     "Karnataka",         2,  11),
    ("Guntur",        "Andhra Pradesh",    3,   8),
    ("Jamshedpur",    "Jharkhand",         3,   9),
    ("Siliguri",      "West Bengal",       3,   9),
    ("Jammu",         "Jammu and Kashmir", 3,   8),
    ("Srinagar",      "Jammu and Kashmir", 3,   7),
    ("Shillong",      "Meghalaya",         3,   5),
    ("Imphal",        "Manipur",           3,   4),
    ("Aizawl",        "Mizoram",           3,   3),
    ("Gangtok",       "Sikkim",            3,   3),
    ("Panaji",        "Goa",               2,   7),
    ("Puducherry",    "Puducherry",        2,   6),
]

# Canonical spellings we consider "correct". The generator will deliberately
# corrupt some of these into the ALIASES below to create dirty data.
CITY_ALIASES = {
    "Bengaluru": ["Bangalore", "bengaluru", "BANGALORE", " Bengaluru "],
    "Mumbai": ["Bombay", "mumbai", "MUMBAI "],
    "Delhi": ["New Delhi", "delhi", "NEW DELHI"],
    "Kolkata": ["Calcutta", "kolkata"],
    "Chennai": ["Madras", "chennai"],
    "Puducherry": ["Pondicherry"],
    "Thiruvananthapuram": ["Trivandrum"],
    "Mysuru": ["Mysore"],
    "Mangaluru": ["Mangalore"],
    "Vadodara": ["Baroda"],
}

# --------------------------------------------------------------------------- #
# Product taxonomy. price_band is (min_inr, median_inr, max_inr) used to build a
# clipped lognormal so most items sit near the median with a realistic tail.
# return_rate is the baseline probability an item from this category is returned.
# weight_share biases which categories appear most often in order items.
# --------------------------------------------------------------------------- #
CATEGORIES = {
    "Mobiles & Accessories": {
        "subcategories": ["Smartphones", "Cases & Covers", "Chargers", "Power Banks", "Earphones"],
        "price_band": (149, 14999, 129999),
        "return_rate": 0.06,
        "weight_share": 16,
        "weight_grams": (30, 400),
    },
    "Electronics": {
        "subcategories": ["Laptops", "Tablets", "Cameras", "Speakers", "Smart Watches", "Monitors"],
        "price_band": (499, 22999, 189999),
        "return_rate": 0.09,
        "weight_share": 11,
        "weight_grams": (150, 8000),
    },
    "Fashion": {
        "subcategories": ["Men Topwear", "Women Ethnic", "Footwear", "Watches", "Bags", "Kids Wear"],
        "price_band": (149, 899, 14999),
        "return_rate": 0.17,
        "weight_share": 20,
        "weight_grams": (100, 1500),
    },
    "Home & Kitchen": {
        "subcategories": ["Cookware", "Storage", "Furnishing", "Decor", "Dining"],
        "price_band": (99, 1299, 39999),
        "return_rate": 0.08,
        "weight_share": 13,
        "weight_grams": (200, 12000),
    },
    "Beauty & Personal Care": {
        "subcategories": ["Skincare", "Haircare", "Makeup", "Fragrance", "Grooming"],
        "price_band": (99, 599, 8999),
        "return_rate": 0.05,
        "weight_share": 9,
        "weight_grams": (50, 800),
    },
    "Appliances": {
        "subcategories": ["Mixer Grinder", "Iron", "Fan", "Microwave", "Water Purifier"],
        "price_band": (599, 3499, 69999),
        "return_rate": 0.07,
        "weight_share": 6,
        "weight_grams": (500, 25000),
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Academic", "Children", "Regional"],
        "price_band": (79, 349, 3999),
        "return_rate": 0.03,
        "weight_share": 6,
        "weight_grams": (120, 1200),
    },
    "Grocery": {
        "subcategories": ["Staples", "Snacks", "Beverages", "Household", "Personal Hygiene"],
        "price_band": (39, 299, 3999),
        "return_rate": 0.04,
        "weight_share": 8,
        "weight_grams": (100, 10000),
    },
    "Toys & Baby": {
        "subcategories": ["Toys", "Diapers", "Baby Care", "School Supplies"],
        "price_band": (99, 699, 12999),
        "return_rate": 0.06,
        "weight_share": 5,
        "weight_grams": (80, 5000),
    },
    "Sports & Fitness": {
        "subcategories": ["Fitness Equipment", "Cricket", "Footwear", "Cycling", "Yoga"],
        "price_band": (149, 1499, 49999),
        "return_rate": 0.08,
        "weight_share": 6,
        "weight_grams": (150, 15000),
    },
}

# Deliberately-malformed category variants (casing/whitespace) for dirty data.
def category_variants(name: str) -> list[str]:
    return [name, name.lower(), name.upper(), f" {name} ", name.replace(" & ", " and ")]

BRANDS = [
    "Araku", "Zivame", "BharatTech", "Kanha", "Nova", "Vira", "Suraj", "Meraki",
    "Tanish", "Ovi", "Bluewood", "Rasa", "Yugen", "Prisma", "Kaya", "Nirvi",
    "Orion", "Zenda", "Aveer", "Trisha", "Sattva", "Indus", "Vayu", "Amara",
]

# --------------------------------------------------------------------------- #
# Names — common Indian first and last names across regions.
# --------------------------------------------------------------------------- #
FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna",
    "Ishaan", "Rohan", "Ananya", "Diya", "Aadhya", "Saanvi", "Pari", "Myra",
    "Ira", "Aisha", "Riya", "Kavya", "Rahul", "Amit", "Priya", "Neha", "Sneha",
    "Pooja", "Karthik", "Suresh", "Ramesh", "Vijay", "Deepak", "Manish", "Anjali",
    "Divya", "Meera", "Nikhil", "Sandeep", "Rajesh", "Kiran", "Lakshmi", "Sunita",
    "Fatima", "Zoya", "Imran", "Farhan", "Harpreet", "Simran", "Gurpreet", "Manpreet",
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Iyer", "Nair", "Reddy", "Rao", "Naidu", "Patel",
    "Shah", "Mehta", "Desai", "Kulkarni", "Joshi", "Deshpande", "Chopra", "Malhotra",
    "Kapoor", "Khanna", "Singh", "Kaur", "Gill", "Das", "Bose", "Banerjee",
    "Chatterjee", "Mukherjee", "Ghosh", "Menon", "Pillai", "Krishnan", "Subramanian",
    "Agarwal", "Bansal", "Jain", "Khan", "Sheikh", "Ansari", "Pawar", "More",
]

PAYMENT_TYPES = ["COD", "UPI", "Credit Card", "Debit Card", "Wallet", "Net Banking"]
PAYMENT_TYPE_WEIGHTS = [0.55, 0.20, 0.10, 0.07, 0.05, 0.03]

# Canonical payment statuses vs the messy variants we inject.
PAYMENT_STATUS_CANONICAL = ["paid", "pending", "failed", "refunded"]
PAYMENT_STATUS_DIRTY = {
    "paid": ["PAID", "Paid", "success", "Success", "completed", "captured"],
    "pending": ["PENDING", "in_process", "Processing"],
    "failed": ["FAILED", "Failed", "declined"],
    "refunded": ["REFUNDED", "Refund", "reversed"],
}

ORDER_STATUSES = ["delivered", "shipped", "processing", "cancelled", "returned"]
ORDER_STATUS_WEIGHTS = [0.80, 0.05, 0.04, 0.07, 0.04]

CARRIERS = ["Ekart", "Delhivery", "BlueDart", "XpressBees", "IndiaPost", "Shadowfax"]

RETURN_REASONS = [
    "Size/fit issue", "Damaged product", "Wrong item delivered",
    "Quality not as expected", "Changed my mind", "Defective/not working",
    "Better price available", "Item arrived late",
]

BUSINESS_TYPES = ["Individual", "Small Business", "Brand Store", "Reseller"]
CUSTOMER_SEGMENTS = ["New", "Occasional", "Regular", "Premium"]
