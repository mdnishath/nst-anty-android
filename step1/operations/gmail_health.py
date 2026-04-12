"""
Gmail Health Activity — Human-like browsing to build Google trust score.

Launches random activities on a logged-in Google profile:
  • Google Search — random queries (country-specific), click results, browse
  • Google Maps — search locations (country-specific), click places
  • YouTube — scroll feed, click video, watch briefly
  • Gmail — open inbox, scroll, read emails
  • Google Drive — visit drive, browse
  • Google Account — visit security settings, click around
  • Google News — browse headlines, click articles
  • Google Shopping — search products, browse

Activities are randomized each run with human-like delays.
Duration and country are configurable.
"""

import asyncio
import random
import time

from shared.logger import _log
from shared.robust import robust_goto

# ─────────────────────────────────────────────────────────────────────────────
# Country-specific content pools
# ─────────────────────────────────────────────────────────────────────────────

COUNTRY_SEARCH_QUERIES = {
    'US': [
        "best restaurants near me", "weather forecast today", "latest news",
        "how to cook pasta carbonara", "funny cat videos", "top movies 2025",
        "best budget smartphones", "morning workout routine", "healthy breakfast ideas",
        "best coffee shops near me", "how to fix a leaky faucet", "national parks to visit",
        "best free online courses", "how to save money fast", "best hiking trails",
        "easy dinner recipes", "best laptops under 500", "how to write a resume",
        "best podcasts 2025", "home office setup ideas", "best running shoes",
        "simple smoothie recipes", "best museums in the world", "travel packing tips",
        "how to tie a tie", "meal prep ideas for the week", "indoor plant care tips",
        "best board games 2025", "best documentaries on netflix",
        "super bowl highlights", "nba scores today", "best pizza near me",
        "amazon deals today", "gas prices near me", "dmv appointment",
    ],
    'FR': [
        "meilleurs restaurants paris", "météo aujourd'hui", "actualités france",
        "recette crêpes bretonnes", "comment faire un gâteau au chocolat",
        "films français 2025", "meilleur smartphone pas cher", "exercices de yoga",
        "idées petit déjeuner sain", "cafés à paris", "comment réparer un robinet",
        "parcs nationaux france", "cours en ligne gratuits", "économiser de l'argent",
        "randonnées en france", "recettes faciles rapides", "meilleur ordinateur portable",
        "comment écrire un cv", "podcasts français populaires", "aménagement bureau maison",
        "chaussures de course", "recettes de smoothies", "musées à visiter en france",
        "conseils voyage", "préparation repas semaine", "plantes d'intérieur",
        "ligue 1 résultats", "tour de france 2025", "boulangerie près de moi",
        "soldes en ligne", "prix essence", "bon plan vacances",
        "recette ratatouille", "horaires sncf", "programme télé ce soir",
        "meilleur fromage français", "vin rouge recommandation", "brocante près de moi",
        "comment faire du pain maison", "jardin potager conseils",
    ],
    'BD': [
        "আজকের খবর", "রান্নার রেসিপি", "ঢাকার রেস্টুরেন্ট",
        "best restaurants dhaka", "weather dhaka today", "bangladesh cricket score",
        "biryani recipe bangla", "hilsa fish recipe", "latest bangla movie",
        "chittagong hill tracts travel", "cox bazar hotel booking",
        "sundarban tour package", "bangladesh news today", "bkash offer today",
        "grameenphone internet offer", "robi recharge offer",
        "dhaka university admission", "online earning bangladesh",
        "ielts preparation tips", "best coaching center dhaka",
        "bangladesh vs india cricket", "bpl 2025 schedule",
        "cheap flights from dhaka", "best mobile under 15000 bdt",
    ],
    'IN': [
        "best restaurants near me", "aaj ka mausam", "latest news india",
        "biryani recipe", "butter chicken recipe", "bollywood movies 2025",
        "best budget phones india", "morning yoga routine", "healthy breakfast indian",
        "best cafes mumbai", "ipl 2025 schedule", "upsc preparation tips",
        "neet exam preparation", "flipkart sale today", "amazon great indian sale",
        "railway pnr status", "petrol price today", "gold rate today",
        "best places to visit india", "goa travel package", "delhi street food",
        "jio recharge plans", "airtel offers today", "aadhaar card update",
        "pan card apply online", "sbi bank balance check", "mutual fund sip",
        "cricket live score", "best dosa near me", "south indian recipes",
    ],
    'DE': [
        "beste Restaurants in meiner Nähe", "Wetter heute", "aktuelle Nachrichten",
        "Rezept Schnitzel", "Schwarzwälder Kirschtorte Rezept", "beste Filme 2025",
        "günstiges Smartphone", "Yoga Übungen Anfänger", "gesundes Frühstück",
        "Cafés in Berlin", "Bundesliga Ergebnisse", "wandern in den Alpen",
        "kostenlose Online-Kurse", "Geld sparen Tipps", "beste Wanderwege Deutschland",
        "einfache Abendessen Rezepte", "bester Laptop unter 500 Euro",
        "Bewerbung schreiben Tipps", "Podcasts auf Deutsch", "Homeoffice einrichten",
        "Laufschuhe Test", "Smoothie Rezepte", "Museen in Deutschland",
        "Reise Packliste", "Meal Prep Ideen", "Zimmerpflanzen Pflege",
        "Deutsche Bahn Fahrplan", "Oktoberfest München", "Weihnachtsmarkt besuchen",
    ],
    'TR': [
        "yakınımdaki restoranlar", "hava durumu bugün", "son dakika haberleri",
        "lahmacun tarifi", "baklava nasıl yapılır", "en iyi filmler 2025",
        "uygun fiyatlı telefon", "yoga hareketleri", "sağlıklı kahvaltı",
        "istanbul kafeleri", "süper lig puan durumu", "kapadokya gezi rehberi",
        "ücretsiz online kurslar", "para biriktirme yolları", "türkiye'de yürüyüş parkurları",
        "kolay yemek tarifleri", "en iyi laptop", "cv nasıl yazılır",
        "türkçe podcastler", "ev ofis düzenleme", "koşu ayakkabısı",
        "smoothie tarifleri", "türkiye müzeleri", "seyahat çantası hazırlama",
        "haftalık yemek hazırlığı", "iç mekan bitkileri bakımı",
        "istanbul'da gezilecek yerler", "antalya otelleri", "türk kahvesi yapımı",
    ],
    'BR': [
        "melhores restaurantes perto de mim", "previsão do tempo hoje", "últimas notícias",
        "receita de feijoada", "bolo de cenoura receita", "melhores filmes 2025",
        "celular bom e barato", "exercícios de yoga", "café da manhã saudável",
        "cafeterias em são paulo", "brasileirão resultados", "trilhas no brasil",
        "cursos online gratuitos", "como economizar dinheiro", "melhores praias brasil",
        "receitas fáceis para jantar", "melhor notebook custo benefício",
        "como fazer currículo", "podcasts em português", "home office organização",
        "tênis para corrida", "receitas de smoothie", "museus no brasil",
        "dicas de viagem", "meal prep receitas", "plantas para apartamento",
        "preço da gasolina hoje", "carnaval 2025", "receita de pão de queijo",
    ],
    'PK': [
        "best restaurants near me", "weather today", "pakistan news today",
        "biryani recipe pakistani", "nihari recipe", "latest pakistani drama",
        "best budget phones pakistan", "cricket live score pakistan",
        "psl 2025 schedule", "cheap flights pakistan", "islamabad restaurants",
        "lahore food street", "karachi beach", "northern areas pakistan travel",
        "hunza valley tour package", "jazz internet packages", "telenor call packages",
        "online earning in pakistan", "freelancing tips urdu",
        "best universities pakistan", "css exam preparation",
        "gold rate today pakistan", "dollar rate today", "petrol price pakistan",
    ],
    'ID': [
        "restoran terdekat", "cuaca hari ini", "berita terkini indonesia",
        "resep nasi goreng", "resep rendang", "film terbaru 2025",
        "hp murah terbaik", "olahraga di rumah", "sarapan sehat",
        "kafe di jakarta", "liga 1 klasemen", "tempat wisata indonesia",
        "kursus online gratis", "cara menabung", "pantai terindah indonesia",
        "resep masakan mudah", "laptop murah terbaik", "cara membuat cv",
        "podcast indonesia", "dekorasi ruang kerja", "sepatu lari terbaik",
        "resep smoothie", "museum di indonesia", "tips traveling",
        "harga bbm hari ini", "promo tokopedia", "resep martabak",
    ],
    'PH': [
        "best restaurants near me", "weather today philippines", "latest news philippines",
        "adobo recipe", "sinigang recipe", "latest pinoy movies 2025",
        "best budget phones philippines", "home workout routine",
        "healthy breakfast ideas", "cafes in manila", "pba scores today",
        "best beaches philippines", "free online courses", "how to save money",
        "boracay travel guide", "easy dinner recipes pinoy", "best laptop for students",
        "how to write a resume", "popular podcasts philippines",
        "work from home setup", "running shoes philippines", "smoothie recipes",
        "museums in manila", "travel tips domestic", "meal prep ideas",
        "indoor plants care", "gcash promo today", "shopee sale schedule",
    ],
    'GB': [
        "best restaurants near me", "weather forecast today", "latest news uk",
        "fish and chips near me", "sunday roast recipe", "best films 2025",
        "best budget phones uk", "morning workout routine", "healthy breakfast ideas",
        "best coffee shops london", "premier league results", "hiking trails uk",
        "free online courses uk", "how to save money uk", "national trust places",
        "easy dinner recipes", "best laptops under 500", "how to write a cv uk",
        "best podcasts 2025", "home office ideas", "best running shoes uk",
        "smoothie recipes", "museums london", "packing list holiday",
        "train times uk", "bbc iplayer", "best pubs near me",
    ],
    'ES': [
        "mejores restaurantes cerca de mí", "el tiempo hoy", "últimas noticias españa",
        "receta paella valenciana", "cómo hacer tortilla española", "mejores películas 2025",
        "móviles baratos y buenos", "rutina de ejercicios en casa", "desayuno saludable ideas",
        "cafeterías en madrid", "resultados la liga", "rutas de senderismo españa",
        "cursos online gratis", "cómo ahorrar dinero rápido", "mejores playas españa",
        "recetas fáciles para cenar", "mejores portátiles calidad precio", "cómo hacer un currículum",
        "podcasts en español populares", "ideas despacho en casa", "zapatillas running baratas",
        "recetas de batidos", "museos en españa", "consejos para viajar",
        "horarios renfe", "precio gasolina hoy", "ofertas amazon españa",
        "tapas recetas caseras", "plan de pensiones", "lotería nacional resultados",
    ],
    'IT': [
        "migliori ristoranti vicino a me", "meteo oggi", "ultime notizie italia",
        "ricetta pasta carbonara", "come fare la pizza in casa", "migliori film 2025",
        "smartphone economici migliori", "esercizi a casa", "colazione sana idee",
        "caffè a roma", "risultati serie a", "sentieri trekking italia",
        "corsi online gratuiti", "come risparmiare soldi", "migliori spiagge italia",
        "ricette facili per cena", "migliori computer portatili", "come scrivere un curriculum",
        "podcast italiani popolari", "arredamento ufficio casa", "scarpe da corsa migliori",
        "ricette frullati", "musei in italia", "consigli di viaggio",
        "orari treni trenitalia", "prezzo benzina oggi", "offerte amazon italia",
        "ricetta tiramisù originale", "ricetta risotto ai funghi", "programmi tv stasera",
    ],
    'PT': [
        "melhores restaurantes perto de mim", "previsão do tempo hoje", "últimas notícias portugal",
        "receita bacalhau à brás", "como fazer pastéis de nata", "melhores filmes 2025",
        "telemóveis baratos e bons", "exercícios em casa", "pequeno almoço saudável",
        "cafés em lisboa", "resultados primeira liga", "trilhos pedestres portugal",
        "cursos online gratuitos", "como poupar dinheiro", "melhores praias portugal",
        "receitas fáceis para jantar", "melhores computadores portáteis", "como fazer currículo",
        "podcasts em português", "ideias escritório em casa", "sapatilhas corrida baratas",
        "receitas de batidos", "museus em portugal", "dicas de viagem",
        "horários comboios cp", "preço gasolina hoje", "promoções continente",
        "receita francesinha porto", "receita caldo verde", "programa tv hoje",
    ],
    'PL': [
        "najlepsze restauracje w pobliżu", "pogoda dzisiaj", "najnowsze wiadomości polska",
        "przepis na pierogi", "jak zrobić bigos", "najlepsze filmy 2025",
        "tanie telefony dobre", "ćwiczenia w domu", "zdrowe śniadanie pomysły",
        "kawiarnie w warszawie", "wyniki ekstraklasa", "szlaki turystyczne polska",
        "kursy online za darmo", "jak oszczędzać pieniądze", "najlepsze plaże polska",
        "łatwe przepisy na kolację", "najlepsze laptopy do 3000 zł", "jak napisać cv",
        "podcasty po polsku", "pomysły na biuro w domu", "buty do biegania ranking",
        "przepisy na koktajle", "muzea w polsce", "porady podróżnicze",
        "rozkład jazdy pkp", "cena benzyny dzisiaj", "promocje allegro",
        "przepis na żurek", "przepis na sernik", "program tv dzisiaj",
    ],
    'NL': [
        "beste restaurants bij mij in de buurt", "weer vandaag", "laatste nieuws nederland",
        "recept stamppot", "hoe maak je pannenkoeken", "beste films 2025",
        "goedkope telefoons", "oefeningen thuis", "gezond ontbijt ideeën",
        "cafés in amsterdam", "eredivisie uitslagen", "wandelroutes nederland",
        "gratis online cursussen", "hoe geld besparen", "beste stranden nederland",
        "makkelijke recepten avondeten", "beste laptops onder 500 euro", "hoe schrijf je een cv",
        "populaire podcasts nederlands", "thuiswerken inrichten", "hardloopschoenen test",
        "smoothie recepten", "musea in nederland", "reistips",
        "ns treinreizen", "benzineprijs vandaag", "aanbiedingen bol.com",
        "recept bitterballen", "recept appeltaart", "tv gids vanavond",
    ],
    'RU': [
        "лучшие рестораны рядом", "погода сегодня", "последние новости россия",
        "рецепт борща", "как приготовить пельмени", "лучшие фильмы 2025",
        "недорогие смартфоны", "упражнения дома", "здоровый завтрак идеи",
        "кафе в москве", "результаты рпл", "маршруты для походов россия",
        "бесплатные онлайн курсы", "как экономить деньги", "лучшие пляжи россии",
        "простые рецепты на ужин", "лучшие ноутбуки до 50000 рублей", "как составить резюме",
        "подкасты на русском", "идеи для домашнего офиса", "кроссовки для бега рейтинг",
        "рецепты смузи", "музеи в россии", "советы путешественникам",
        "расписание поездов ржд", "цена бензина сегодня", "скидки на озон",
        "рецепт оливье", "рецепт блинов", "программа тв сегодня",
    ],
    'JP': [
        "近くのおすすめレストラン", "今日の天気", "最新ニュース 日本",
        "カレーライス レシピ", "ラーメン 作り方", "おすすめ映画 2025",
        "安いスマートフォン おすすめ", "自宅でできる運動", "健康的な朝食 アイデア",
        "東京 カフェ おすすめ", "Jリーグ 結果", "日本 ハイキングコース",
        "無料オンライン講座", "お金を貯める方法", "日本 おすすめビーチ",
        "簡単 夕食レシピ", "おすすめノートパソコン", "履歴書の書き方",
        "人気ポッドキャスト 日本語", "ホームオフィス インテリア", "ランニングシューズ おすすめ",
        "スムージー レシピ", "日本の美術館", "旅行 アドバイス",
        "新幹線 時刻表", "ガソリン 価格 今日", "アマゾン セール",
        "お好み焼き レシピ", "味噌汁 作り方", "今日のテレビ番組",
    ],
    'KR': [
        "근처 맛집 추천", "오늘 날씨", "최신 뉴스 한국",
        "김치찌개 레시피", "불고기 만드는 법", "추천 영화 2025",
        "가성비 스마트폰 추천", "집에서 할 수 있는 운동", "건강한 아침식사 아이디어",
        "서울 카페 추천", "K리그 결과", "한국 등산 코스 추천",
        "무료 온라인 강좌", "돈 모으는 방법", "한국 해수욕장 추천",
        "간단한 저녁 레시피", "추천 노트북 가성비", "이력서 작성법",
        "인기 팟캐스트 한국어", "홈오피스 인테리어", "러닝화 추천",
        "스무디 레시피", "한국 박물관 추천", "여행 팁",
        "KTX 시간표", "오늘 기름값", "쿠팡 세일",
        "떡볶이 레시피", "된장찌개 만드는 법", "오늘 TV 편성표",
    ],
    'MX': [
        "mejores restaurantes cerca de mí", "clima hoy", "últimas noticias méxico",
        "receta tacos al pastor", "cómo hacer mole poblano", "mejores películas 2025",
        "celulares baratos y buenos", "rutina de ejercicio en casa", "desayuno saludable ideas",
        "cafeterías en cdmx", "resultados liga mx", "rutas de senderismo méxico",
        "cursos en línea gratis", "cómo ahorrar dinero", "mejores playas méxico",
        "recetas fáciles para cenar", "mejores laptops calidad precio", "cómo hacer un currículum",
        "podcasts en español méxico", "ideas oficina en casa", "tenis para correr baratos",
        "recetas de licuados", "museos en méxico", "tips para viajar",
        "precio gasolina hoy", "ofertas mercado libre", "receta enchiladas verdes",
        "receta pozole rojo", "receta chilaquiles", "programación tv hoy",
    ],
    'AR': [
        "mejores restaurantes cerca de mí", "clima hoy", "últimas noticias argentina",
        "receta empanadas argentinas", "cómo hacer asado", "mejores películas 2025",
        "celulares baratos y buenos", "rutina de ejercicios en casa", "desayuno saludable ideas",
        "cafeterías en buenos aires", "resultados liga profesional", "rutas de senderismo argentina",
        "cursos online gratis", "cómo ahorrar plata", "mejores playas argentina",
        "recetas fáciles para cenar", "mejores notebooks calidad precio", "cómo hacer un cv",
        "podcasts en español argentina", "ideas oficina en casa", "zapatillas running baratas",
        "recetas de licuados", "museos en argentina", "tips para viajar",
        "precio nafta hoy", "ofertas mercado libre", "receta milanesas napolitana",
        "receta locro argentino", "receta alfajores", "programación tv hoy",
    ],
    'SA': [
        "أفضل المطاعم القريبة", "حالة الطقس اليوم", "آخر الأخبار السعودية",
        "طريقة عمل الكبسة", "طريقة عمل المندي", "أفضل الأفلام 2025",
        "أفضل الجوالات الرخيصة", "تمارين رياضية في المنزل", "أفكار فطور صحي",
        "مقاهي في الرياض", "نتائج الدوري السعودي", "أماكن سياحية في السعودية",
        "دورات مجانية عبر الإنترنت", "كيف توفر المال", "أفضل الشواطئ في السعودية",
        "وصفات عشاء سهلة", "أفضل اللابتوبات", "كيف تكتب سيرة ذاتية",
        "بودكاست عربي", "أفكار مكتب منزلي", "أحذية جري مريحة",
        "وصفات سموذي", "متاحف السعودية", "نصائح سفر",
        "سعر البنزين اليوم", "عروض نون", "طريقة عمل الجريش",
        "طريقة عمل المطبق", "وصفات حلويات سهلة", "برامج التلفزيون اليوم",
    ],
    'AE': [
        "أفضل المطاعم القريبة", "حالة الطقس اليوم", "آخر الأخبار الإمارات",
        "طريقة عمل المچبوس", "طريقة عمل اللقيمات", "أفضل الأفلام 2025",
        "أفضل الجوالات الرخيصة", "تمارين رياضية في المنزل", "أفكار فطور صحي",
        "مقاهي في دبي", "نتائج الدوري الإماراتي", "أماكن سياحية في الإمارات",
        "دورات مجانية عبر الإنترنت", "كيف توفر المال", "أفضل الشواطئ في دبي",
        "وصفات عشاء سهلة", "أفضل اللابتوبات", "كيف تكتب سيرة ذاتية",
        "بودكاست عربي شهير", "أفكار مكتب منزلي", "أحذية جري مريحة",
        "وصفات سموذي", "متاحف الإمارات", "نصائح سفر",
        "سعر البنزين اليوم دبي", "عروض نون الإمارات", "طريقة عمل الهريس",
        "طريقة عمل البلاليط", "وصفات حلويات إماراتية", "برامج التلفزيون اليوم",
    ],
    'EG': [
        "أفضل المطاعم القريبة", "حالة الطقس اليوم", "آخر الأخبار مصر",
        "طريقة عمل الكشري", "طريقة عمل الملوخية", "أفضل الأفلام 2025",
        "أفضل الموبايلات الرخيصة", "تمارين رياضية في البيت", "أفكار فطار صحي",
        "كافيهات في القاهرة", "نتائج الدوري المصري", "أماكن سياحية في مصر",
        "كورسات مجانية أونلاين", "إزاي توفر فلوس", "أحسن شواطئ في مصر",
        "وصفات عشاء سهلة", "أحسن لابتوب", "إزاي تكتب سي في",
        "بودكاست عربي مصري", "أفكار مكتب في البيت", "جزم جري مريحة",
        "وصفات سموذي", "متاحف مصر", "نصائح سفر",
        "سعر البنزين النهاردة", "عروض جوميا", "طريقة عمل الفتة المصرية",
        "طريقة عمل أم علي", "وصفات حلويات سهلة", "برامج التلفزيون النهاردة",
    ],
    'NG': [
        "best restaurants near me", "weather forecast today", "latest news nigeria",
        "jollof rice recipe", "how to make egusi soup", "best nollywood movies 2025",
        "best budget phones nigeria", "home workout routine", "healthy breakfast ideas",
        "best cafes in lagos", "premier league results", "tourist attractions nigeria",
        "free online courses", "how to save money naira", "best beaches nigeria",
        "easy dinner recipes nigerian", "best laptops under 300k naira", "how to write a cv",
        "popular podcasts nigeria", "home office ideas", "best running shoes",
        "smoothie recipes", "museums in lagos", "travel tips nigeria",
        "fuel price today", "jumia deals today", "suya recipe",
        "pounded yam recipe", "chin chin recipe", "tv schedule today",
    ],
    'ZA': [
        "best restaurants near me", "weather forecast today", "latest news south africa",
        "braai recipes", "bobotie recipe", "best movies 2025",
        "best budget phones south africa", "home workout routine", "healthy breakfast ideas",
        "best coffee shops cape town", "psl results today", "hiking trails south africa",
        "free online courses", "how to save money rands", "best beaches south africa",
        "easy dinner recipes", "best laptops under 10000 rand", "how to write a cv south africa",
        "popular podcasts south africa", "home office ideas", "best running shoes",
        "smoothie recipes", "museums in south africa", "travel tips south africa",
        "petrol price today", "takealot deals", "biltong recipe",
        "melktert recipe", "koeksister recipe", "tv schedule today",
    ],
    'AU': [
        "best restaurants near me", "weather forecast today", "latest news australia",
        "meat pie recipe", "lamington recipe", "best movies 2025",
        "best budget phones australia", "morning workout routine", "healthy breakfast ideas",
        "best coffee shops melbourne", "afl results today", "hiking trails australia",
        "free online courses australia", "how to save money fast", "best beaches australia",
        "easy dinner recipes", "best laptops under 1000 aud", "how to write a resume australia",
        "best podcasts 2025", "home office setup ideas", "best running shoes australia",
        "smoothie recipes", "museums in sydney", "travel tips australia",
        "woolworths specials", "petrol prices today", "bunnings hardware",
        "tim tam recipe", "pavlova recipe", "tv guide tonight",
    ],
    'CA': [
        "best restaurants near me", "weather forecast today", "latest news canada",
        "poutine recipe", "butter tart recipe", "best movies 2025",
        "best budget phones canada", "morning workout routine", "healthy breakfast ideas",
        "best coffee shops toronto", "nhl scores today", "hiking trails canada",
        "free online courses canada", "how to save money canada", "best beaches canada",
        "easy dinner recipes", "best laptops under 1000 cad", "how to write a resume canada",
        "best podcasts 2025", "home office setup ideas", "best running shoes canada",
        "smoothie recipes", "museums in ottawa", "travel tips canada",
        "gas prices today", "canadian tire deals", "shoppers drug mart flyer",
        "nanaimo bar recipe", "tourtiere recipe", "cbc schedule tonight",
    ],
}

COUNTRY_MAP_LOCATIONS = {
    'US': [
        "Times Square New York", "Golden Gate Bridge", "Statue of Liberty",
        "Central Park NYC", "Grand Canyon Arizona", "Hollywood Sign LA",
        "Empire State Building", "Yellowstone National Park", "Miami Beach",
        "Las Vegas Strip", "Disney World Orlando", "Space Needle Seattle",
    ],
    'FR': [
        "Tour Eiffel Paris", "Louvre Museum Paris", "Notre-Dame de Paris",
        "Mont Saint-Michel", "Château de Versailles", "Arc de Triomphe Paris",
        "Côte d'Azur Nice", "Sacré-Cœur Montmartre", "Pont du Gard",
        "Carcassonne", "Strasbourg Cathedral", "Lyon Vieux Lyon",
        "Marseille Vieux-Port", "Bordeaux centre-ville", "Toulouse Capitole",
    ],
    'BD': [
        "Cox's Bazar Beach", "Sundarbans Bangladesh", "Lalbagh Fort Dhaka",
        "Ahsan Manzil Dhaka", "Srimangal Tea Gardens", "Saint Martin Island",
        "Ratargul Swamp Forest", "Paharpur Bihar", "Rangamati Lake",
        "Kuakata Beach", "Sonargaon", "Mahasthangarh",
    ],
    'IN': [
        "Taj Mahal Agra", "India Gate New Delhi", "Gateway of India Mumbai",
        "Hawa Mahal Jaipur", "Qutub Minar Delhi", "Red Fort Delhi",
        "Mysore Palace", "Golden Temple Amritsar", "Goa Beaches",
        "Kerala Backwaters", "Varanasi Ghats", "Hampi Ruins",
    ],
    'DE': [
        "Brandenburg Gate Berlin", "Neuschwanstein Castle", "Cologne Cathedral",
        "Berlin Wall Memorial", "Munich Marienplatz", "Heidelberg Castle",
        "Hamburg Speicherstadt", "Dresden Frauenkirche", "Black Forest",
        "Rhine Valley", "Zugspitze", "Rothenburg ob der Tauber",
    ],
    'TR': [
        "Hagia Sophia Istanbul", "Blue Mosque Istanbul", "Cappadocia",
        "Ephesus Ruins", "Topkapi Palace", "Grand Bazaar Istanbul",
        "Pamukkale Travertines", "Antalya Old Town", "Galata Tower",
        "Mount Nemrut", "Troy Ancient City", "Bodrum Castle",
    ],
    'BR': [
        "Christ the Redeemer Rio", "Sugarloaf Mountain", "Copacabana Beach",
        "Iguazu Falls Brazil", "Amazon Rainforest Manaus", "São Paulo Paulista",
        "Pelourinho Salvador", "Chapada Diamantina", "Fernando de Noronha",
        "Ouro Preto", "Lençóis Maranhenses", "Foz do Iguaçu",
    ],
    'PK': [
        "Badshahi Mosque Lahore", "Faisal Mosque Islamabad", "Minar-e-Pakistan",
        "Hunza Valley", "Skardu Pakistan", "Mohenjo-daro", "Lahore Fort",
        "Fairy Meadows", "Nanga Parbat Base Camp", "Swat Valley",
        "Taxila Museum", "Karachi Clifton Beach",
    ],
    'ID': [
        "Bali Tanah Lot Temple", "Borobudur Temple Java", "Komodo Island",
        "Raja Ampat Papua", "Ubud Monkey Forest", "Bromo Mountain",
        "Jakarta Monas", "Yogyakarta Kraton", "Lake Toba Sumatra",
        "Prambanan Temple", "Gili Islands", "Tana Toraja",
    ],
    'PH': [
        "Chocolate Hills Bohol", "Mayon Volcano", "Intramuros Manila",
        "El Nido Palawan", "Boracay Beach", "Banaue Rice Terraces",
        "Coron Palawan", "Cebu Basilica", "Vigan Heritage City",
        "Siargao Island", "Puerto Princesa Underground River", "Taal Volcano",
    ],
    'GB': [
        "Big Ben London", "Buckingham Palace", "Tower of London",
        "Stonehenge", "Edinburgh Castle", "Bath Roman Baths",
        "Windsor Castle", "Lake District", "Oxford University",
        "Cambridge Colleges", "Stratford-upon-Avon", "Giant's Causeway",
    ],
    'ES': [
        "Sagrada Familia Barcelona", "Alhambra Granada", "Plaza Mayor Madrid",
        "Mezquita Córdoba", "Parque Güell Barcelona", "Museo del Prado Madrid",
        "Catedral Sevilla", "Acueducto Segovia", "Santiago de Compostela",
        "San Sebastián Playa", "Tenerife Teide", "Mallorca Palma",
    ],
    'IT': [
        "Colosseo Roma", "Piazza San Marco Venezia", "Duomo di Milano",
        "Torre di Pisa", "Ponte Vecchio Firenze", "Fontana di Trevi Roma",
        "Costiera Amalfitana", "Pompei Scavi", "Vaticano Roma",
        "Cinque Terre Liguria", "Lago di Como", "Piazza del Campo Siena",
    ],
    'PT': [
        "Torre de Belém Lisboa", "Ponte Dom Luís Porto", "Palácio da Pena Sintra",
        "Praça do Comércio Lisboa", "Mosteiro dos Jerónimos", "Cabo da Roca",
        "Universidade de Coimbra", "Algarve Faro", "Braga Bom Jesus",
        "Ribeira Porto", "Évora Templo Romano", "Madeira Funchal",
    ],
    'PL': [
        "Rynek Główny Kraków", "Zamek Królewski Warszawa", "Wawel Kraków",
        "Stare Miasto Gdańsk", "Auschwitz Oświęcim", "Kopalnia Soli Wieliczka",
        "Łazienki Królewskie Warszawa", "Malbork Zamek", "Tatry Zakopane",
        "Wrocław Rynek", "Poznań Stary Rynek", "Toruń Stare Miasto",
    ],
    'NL': [
        "Rijksmuseum Amsterdam", "Anne Frank Huis Amsterdam", "Vondelpark Amsterdam",
        "Keukenhof Lisse", "Kinderdijk Windmills", "Van Gogh Museum Amsterdam",
        "Dam Square Amsterdam", "Mauritshuis Den Haag", "Markthal Rotterdam",
        "Utrecht Domtoren", "Maastricht Vrijthof", "Giethoorn Village",
    ],
    'RU': [
        "Красная площадь Москва", "Эрмитаж Санкт-Петербург", "Кремль Москва",
        "Петергоф", "Храм Василия Блаженного", "Дворцовая площадь",
        "Байкал озеро", "Невский проспект", "Третьяковская галерея",
        "Казанский Кремль", "Мамаев курган Волгоград", "Сочи набережная",
    ],
    'JP': [
        "東京タワー", "浅草寺 東京", "京都 金閣寺",
        "富士山", "大阪城", "厳島神社 広島",
        "東京スカイツリー", "奈良 東大寺", "渋谷スクランブル交差点",
        "清水寺 京都", "沖縄 美ら海水族館", "札幌 時計台",
    ],
    'KR': [
        "경복궁 서울", "남산타워 서울", "해운대 해수욕장 부산",
        "제주도 한라산", "광화문 광장", "불국사 경주",
        "명동 서울", "인사동 서울", "한옥마을 전주",
        "동대문 디자인 플라자", "성산일출봉 제주", "북촌 한옥마을",
    ],
    'MX': [
        "Chichén Itzá Yucatán", "Teotihuacán México", "Zócalo Ciudad de México",
        "Cancún Playa", "Frida Kahlo Museum CDMX", "Tulum Ruins",
        "Guadalajara Centro", "Oaxaca Centro", "San Miguel de Allende",
        "Palacio de Bellas Artes CDMX", "Puerto Vallarta", "Playa del Carmen",
    ],
    'AR': [
        "Obelisco Buenos Aires", "Caminito La Boca", "Plaza de Mayo Buenos Aires",
        "Glaciar Perito Moreno", "Cataratas del Iguazú", "Teatro Colón Buenos Aires",
        "Bariloche Centro Cívico", "Mendoza Viñedos", "Ushuaia Fin del Mundo",
        "Quebrada de Humahuaca", "Recoleta Cementerio", "Puerto Madero Buenos Aires",
    ],
    'SA': [
        "الحرم المكي مكة", "المسجد النبوي المدينة", "برج المملكة الرياض",
        "واجهة الرياض", "العلا مدائن صالح", "جدة البلد التاريخية",
        "الدرعية التاريخية", "حافة العالم الرياض", "كورنيش جدة",
        "جزيرة فرسان", "أبها السودة", "ينبع الشاطئ",
    ],
    'AE': [
        "برج خليفة دبي", "متحف اللوفر أبوظبي", "نخلة جميرا دبي",
        "دبي مول", "مسجد الشيخ زايد أبوظبي", "برج العرب دبي",
        "القرية العالمية دبي", "عين دبي", "جزيرة ياس أبوظبي",
        "خور دبي", "سوق الذهب دبي", "مارينا دبي",
    ],
    'EG': [
        "أهرامات الجيزة", "معبد الكرنك الأقصر", "المتحف المصري القاهرة",
        "وادي الملوك الأقصر", "الإسكندرية قلعة قايتباي", "أبو سمبل أسوان",
        "خان الخليلي القاهرة", "شرم الشيخ", "الغردقة البحر الأحمر",
        "مسجد محمد علي القاهرة", "واحة سيوة", "دهب سيناء",
    ],
    'NG': [
        "Lekki Conservation Centre Lagos", "Nike Art Gallery Lagos", "Olumo Rock Abeokuta",
        "National Museum Lagos", "Yankari Game Reserve", "Zuma Rock Abuja",
        "Aso Rock Abuja", "Osun-Osogbo Sacred Grove", "Obudu Mountain Resort",
        "Tarkwa Bay Beach Lagos", "Millennium Park Abuja", "Tinapa Resort Calabar",
    ],
    'ZA': [
        "Table Mountain Cape Town", "Kruger National Park", "V&A Waterfront Cape Town",
        "Robben Island", "Johannesburg Apartheid Museum", "Blyde River Canyon",
        "Garden Route", "Durban Beachfront", "Stellenbosch Wine Route",
        "Drakensberg Mountains", "Pilanesberg National Park", "Addo Elephant Park",
    ],
    'AU': [
        "Sydney Opera House", "Great Barrier Reef Queensland", "Uluru Ayers Rock",
        "Sydney Harbour Bridge", "Melbourne Federation Square", "Blue Mountains NSW",
        "Bondi Beach Sydney", "Twelve Apostles Victoria", "Kakadu National Park",
        "Daintree Rainforest", "Perth Kings Park", "Hobart MONA",
    ],
    'CA': [
        "Niagara Falls Ontario", "CN Tower Toronto", "Banff National Park",
        "Old Montreal Quebec", "Stanley Park Vancouver", "Parliament Hill Ottawa",
        "Whistler BC", "Peggy's Cove Nova Scotia", "Lake Louise Alberta",
        "Signal Hill Newfoundland", "Quebec City Old Town", "Butchart Gardens Victoria",
    ],
}

COUNTRY_YOUTUBE_SEARCHES = {
    'US': [
        "relaxing music", "cooking tutorial", "travel vlog", "tech review 2025",
        "how to draw", "funny animals compilation", "science experiment",
        "workout at home", "guitar lesson beginner", "nature documentary",
    ],
    'FR': [
        "musique relaxante", "recette cuisine française", "vlog voyage france",
        "test tech 2025", "tuto dessin", "animaux drôles compilation",
        "expérience scientifique", "sport à la maison", "cours de guitare",
        "documentaire nature france",
    ],
    'BD': [
        "bangla song", "bangladeshi cooking recipe", "dhaka city vlog",
        "mobile review bangla", "drawing tutorial bangla", "funny bangla video",
        "cricket highlights bangladesh", "home workout bangla",
        "bangla natok new", "travel vlog bangladesh",
    ],
    'IN': [
        "bollywood songs", "indian cooking recipe", "travel vlog india",
        "tech review hindi", "drawing tutorial", "funny indian videos",
        "cricket highlights india", "yoga at home hindi",
        "guitar lesson hindi", "indian wildlife documentary",
    ],
    'DE': [
        "entspannende Musik", "Kochen lernen deutsch", "Reise Vlog Deutschland",
        "Tech Review deutsch", "Zeichnen lernen", "lustige Tier Videos",
        "Wissenschaft Experiment", "Sport zu Hause", "Gitarre lernen",
        "Natur Dokumentation",
    ],
    'TR': [
        "rahatlatıcı müzik", "yemek tarifi türk mutfağı", "gezi vlog türkiye",
        "teknoloji inceleme 2025", "resim çizme dersi", "komik hayvan videoları",
        "bilim deneyi", "evde spor", "gitar dersi", "doğa belgeseli",
    ],
    'BR': [
        "música relaxante", "receita culinária brasileira", "vlog viagem brasil",
        "review tecnologia 2025", "tutorial desenho", "animais engraçados",
        "experiência científica", "treino em casa", "aula de violão",
        "documentário natureza brasil",
    ],
    'PK': [
        "pakistani songs", "pakistani cooking recipe", "travel vlog pakistan",
        "mobile review urdu", "drawing tutorial urdu", "funny pakistani videos",
        "cricket highlights pakistan", "home workout urdu",
        "naat sharif", "northern areas pakistan travel",
    ],
    'ID': [
        "musik santai", "resep masakan indonesia", "travel vlog indonesia",
        "review teknologi 2025", "tutorial menggambar", "video lucu binatang",
        "eksperimen sains", "olahraga di rumah", "belajar gitar",
        "dokumenter alam indonesia",
    ],
    'PH': [
        "opm music", "filipino cooking recipe", "travel vlog philippines",
        "tech review 2025 filipino", "drawing tutorial", "funny pinoy videos",
        "basketball highlights pba", "home workout", "guitar tutorial tagalog",
        "nature documentary philippines",
    ],
    'GB': [
        "relaxing music", "british cooking recipe", "travel vlog uk",
        "tech review 2025", "drawing tutorial", "funny animal videos",
        "science experiment", "home workout", "guitar lesson beginner",
        "nature documentary bbc",
    ],
    'ES': [
        "música relajante", "receta cocina española", "vlog viaje españa",
        "análisis tecnología 2025", "tutorial dibujo", "videos graciosos animales",
        "experimento ciencia", "ejercicios en casa", "clase guitarra principiante",
        "documental naturaleza españa", "resumen la liga", "maquillaje tutorial español",
    ],
    'IT': [
        "musica rilassante", "ricetta cucina italiana", "vlog viaggio italia",
        "recensione tecnologia 2025", "tutorial disegno", "video divertenti animali",
        "esperimento scientifico", "allenamento a casa", "lezione chitarra principiante",
        "documentario natura italia", "serie a highlights", "tutorial trucco italiano",
    ],
    'PT': [
        "música relaxante", "receita culinária portuguesa", "vlog viagem portugal",
        "review tecnologia 2025", "tutorial desenho", "vídeos engraçados animais",
        "experiência científica", "treino em casa", "aula guitarra iniciante",
        "documentário natureza portugal", "liga portugal resumo", "maquilhagem tutorial",
    ],
    'PL': [
        "muzyka relaksacyjna", "przepis kuchnia polska", "vlog podróże polska",
        "recenzja technologia 2025", "tutorial rysowanie", "śmieszne filmy ze zwierzętami",
        "eksperyment naukowy", "ćwiczenia w domu", "lekcja gitary dla początkujących",
        "dokument przyrodniczy polska", "ekstraklasa skróty", "makijaż tutorial polski",
    ],
    'NL': [
        "ontspannende muziek", "recept Nederlandse keuken", "vlog reizen nederland",
        "tech review 2025 nederlands", "teken tutorial", "grappige dierenvideo's",
        "wetenschappelijk experiment", "thuis sporten", "gitaarles beginners",
        "natuurdocumentaire nederland", "eredivisie samenvatting", "make-up tutorial nederlands",
    ],
    'RU': [
        "расслабляющая музыка", "рецепт русская кухня", "влог путешествие россия",
        "обзор технологий 2025", "урок рисования", "смешные видео с животными",
        "научный эксперимент", "тренировка дома", "урок гитары для начинающих",
        "документальный фильм природа", "рпл обзор матча", "макияж урок русский",
    ],
    'JP': [
        "リラックス音楽", "和食 レシピ 作り方", "旅行Vlog 日本",
        "テック レビュー 2025", "イラスト 描き方", "面白い動物動画",
        "科学実験", "自宅トレーニング", "ギター 初心者レッスン",
        "自然ドキュメンタリー 日本", "Jリーグ ハイライト", "メイク チュートリアル",
    ],
    'KR': [
        "편안한 음악", "한국 요리 레시피", "여행 브이로그 한국",
        "테크 리뷰 2025", "그림 그리기 튜토리얼", "재미있는 동물 영상",
        "과학 실험", "홈트레이닝", "기타 초보 레슨",
        "자연 다큐멘터리 한국", "K리그 하이라이트", "메이크업 튜토리얼 한국",
    ],
    'MX': [
        "música relajante", "receta comida mexicana", "vlog viaje méxico",
        "review tecnología 2025", "tutorial dibujo", "videos graciosos de animales",
        "experimento de ciencia", "ejercicio en casa", "clase de guitarra principiante",
        "documental naturaleza méxico", "liga mx resumen", "maquillaje tutorial mexicano",
    ],
    'AR': [
        "música relajante", "receta comida argentina", "vlog viaje argentina",
        "review tecnología 2025", "tutorial de dibujo", "videos graciosos de animales",
        "experimento de ciencia", "ejercicio en casa", "clase de guitarra principiante",
        "documental naturaleza argentina", "liga profesional resumen", "maquillaje tutorial",
    ],
    'SA': [
        "موسيقى هادئة", "وصفة طبخ سعودي", "فلوق سفر السعودية",
        "مراجعة تقنية 2025", "تعليم الرسم", "فيديوهات حيوانات مضحكة",
        "تجربة علمية", "تمارين في البيت", "تعليم العود للمبتدئين",
        "وثائقي طبيعة", "دوري روشن ملخص", "مكياج تعليمي عربي",
    ],
    'AE': [
        "موسيقى هادئة", "وصفة طبخ إماراتي", "فلوق سفر الإمارات",
        "مراجعة تقنية 2025", "تعليم الرسم", "فيديوهات حيوانات مضحكة",
        "تجربة علمية", "تمارين في البيت", "تعليم الجيتار للمبتدئين",
        "وثائقي طبيعة", "الدوري الإماراتي ملخص", "مكياج تعليمي",
    ],
    'EG': [
        "موسيقى هادئة", "وصفة أكل مصري", "فلوق سفر مصر",
        "مراجعة موبايلات 2025", "تعليم رسم", "فيديوهات حيوانات مضحكة",
        "تجربة علمية", "تمارين في البيت", "تعلم جيتار للمبتدئين",
        "وثائقي طبيعة مصر", "الدوري المصري ملخص", "ميكب توتوريال",
    ],
    'NG': [
        "afrobeats music", "nigerian cooking recipe", "travel vlog nigeria",
        "tech review 2025 nigeria", "drawing tutorial", "funny nigerian videos",
        "science experiment", "home workout", "guitar lesson beginner",
        "wildlife documentary africa", "premier league highlights", "makeup tutorial nigerian",
    ],
    'ZA': [
        "relaxing music", "south african cooking recipe", "travel vlog south africa",
        "tech review 2025", "drawing tutorial", "funny animal videos south africa",
        "science experiment", "home workout routine", "guitar lesson beginner",
        "nature documentary south africa", "psl highlights", "makeup tutorial south african",
    ],
    'AU': [
        "relaxing music", "australian cooking recipe", "travel vlog australia",
        "tech review 2025", "drawing tutorial", "funny animal videos australia",
        "science experiment", "home workout", "guitar lesson beginner",
        "nature documentary australia", "afl highlights", "makeup tutorial australian",
    ],
    'CA': [
        "relaxing music", "canadian cooking recipe", "travel vlog canada",
        "tech review 2025", "drawing tutorial", "funny animal videos",
        "science experiment", "home workout", "guitar lesson beginner",
        "nature documentary canada", "nhl highlights", "makeup tutorial canadian",
    ],
}

COUNTRY_SHOPPING_QUERIES = {
    'US': [
        "wireless earbuds", "running shoes", "laptop stand", "water bottle",
        "desk lamp", "backpack", "phone case", "headphones", "webcam",
    ],
    'FR': [
        "écouteurs sans fil", "chaussures de course", "support ordinateur",
        "bouteille d'eau", "lampe de bureau", "sac à dos", "coque téléphone",
        "casque audio", "webcam",
    ],
    'BD': [
        "wireless earbuds", "running shoes", "mobile cover", "power bank",
        "desk lamp", "backpack", "laptop bag", "headphones", "smartwatch",
    ],
    'IN': [
        "wireless earbuds india", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "smartwatch under 5000",
    ],
    'DE': [
        "kabellose Kopfhörer", "Laufschuhe", "Laptopständer",
        "Trinkflasche", "Schreibtischlampe", "Rucksack", "Handyhülle",
        "Kopfhörer", "Webcam",
    ],
    'TR': [
        "kablosuz kulaklık", "koşu ayakkabısı", "laptop standı",
        "su şişesi", "masa lambası", "sırt çantası", "telefon kılıfı",
        "kulaklık", "webcam",
    ],
    'BR': [
        "fone bluetooth", "tênis corrida", "suporte notebook",
        "garrafa de água", "luminária mesa", "mochila", "capinha celular",
        "headphone", "webcam",
    ],
    'PK': [
        "wireless earbuds pakistan", "running shoes", "laptop bag",
        "power bank", "desk lamp", "backpack", "mobile cover",
        "headphones", "smartwatch",
    ],
    'ID': [
        "earbuds wireless", "sepatu lari", "stand laptop",
        "botol minum", "lampu meja", "tas ransel", "case hp",
        "headphone", "webcam",
    ],
    'PH': [
        "wireless earbuds", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam",
    ],
    'GB': [
        "wireless earbuds uk", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam",
    ],
    'ES': [
        "auriculares inalámbricos", "zapatillas running", "soporte portátil",
        "botella de agua", "lámpara escritorio", "mochila", "funda móvil",
        "cascos bluetooth", "webcam", "ratón inalámbrico", "teclado mecánico",
    ],
    'IT': [
        "auricolari wireless", "scarpe da corsa", "supporto laptop",
        "borraccia", "lampada scrivania", "zaino", "cover telefono",
        "cuffie bluetooth", "webcam", "mouse wireless", "tastiera meccanica",
    ],
    'PT': [
        "auriculares sem fios", "sapatilhas corrida", "suporte portátil",
        "garrafa de água", "candeeiro secretária", "mochila", "capa telemóvel",
        "auscultadores bluetooth", "webcam", "rato sem fios", "teclado mecânico",
    ],
    'PL': [
        "słuchawki bezprzewodowe", "buty do biegania", "podstawka pod laptopa",
        "butelka na wodę", "lampka biurkowa", "plecak", "etui na telefon",
        "słuchawki bluetooth", "kamerka internetowa", "myszka bezprzewodowa", "klawiatura mechaniczna",
    ],
    'NL': [
        "draadloze oordopjes", "hardloopschoenen", "laptop standaard",
        "waterfles", "bureaulamp", "rugzak", "telefoonhoesje",
        "koptelefoon bluetooth", "webcam", "draadloze muis", "mechanisch toetsenbord",
    ],
    'RU': [
        "беспроводные наушники", "кроссовки для бега", "подставка для ноутбука",
        "бутылка для воды", "настольная лампа", "рюкзак", "чехол для телефона",
        "наушники bluetooth", "веб камера", "беспроводная мышь", "механическая клавиатура",
    ],
    'JP': [
        "ワイヤレスイヤホン", "ランニングシューズ", "ノートパソコンスタンド",
        "水筒", "デスクライト", "リュックサック", "スマホケース",
        "ヘッドフォン", "ウェブカメラ", "ワイヤレスマウス", "メカニカルキーボード",
    ],
    'KR': [
        "무선 이어폰", "러닝화", "노트북 거치대",
        "물병", "책상 조명", "백팩", "핸드폰 케이스",
        "블루투스 헤드폰", "웹캠", "무선 마우스", "기계식 키보드",
    ],
    'MX': [
        "audífonos inalámbricos", "tenis para correr", "soporte para laptop",
        "botella de agua", "lámpara de escritorio", "mochila", "funda celular",
        "audífonos bluetooth", "webcam", "mouse inalámbrico", "teclado mecánico",
    ],
    'AR': [
        "auriculares inalámbricos", "zapatillas running", "soporte notebook",
        "botella de agua", "lámpara escritorio", "mochila", "funda celular",
        "auriculares bluetooth", "webcam", "mouse inalámbrico", "teclado mecánico",
    ],
    'SA': [
        "سماعات بلوتوث", "حذاء جري", "حامل لابتوب",
        "قارورة ماء", "مصباح مكتب", "حقيبة ظهر", "كفر جوال",
        "سماعات رأس", "كاميرا ويب", "ماوس لاسلكي", "كيبورد ميكانيكي",
    ],
    'AE': [
        "سماعات بلوتوث", "حذاء رياضي", "حامل لابتوب",
        "قارورة ماء", "مصباح مكتبي", "شنطة ظهر", "كفر موبايل",
        "سماعات رأس", "كاميرا ويب", "ماوس لاسلكي", "كيبورد ميكانيكي",
    ],
    'EG': [
        "سماعات بلوتوث", "جزمة جري", "حامل لاب توب",
        "زجاجة مية", "لمبة مكتب", "شنطة ضهر", "جراب موبايل",
        "سماعات هيدفون", "كاميرا ويب", "ماوس وايرلس", "كيبورد ميكانيكال",
    ],
    'NG': [
        "wireless earbuds", "running shoes nigeria", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam", "power bank", "smartwatch",
    ],
    'ZA': [
        "wireless earbuds south africa", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam", "power bank", "smartwatch",
    ],
    'AU': [
        "wireless earbuds australia", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam", "mechanical keyboard", "smartwatch",
    ],
    'CA': [
        "wireless earbuds canada", "running shoes", "laptop stand",
        "water bottle", "desk lamp", "backpack", "phone case",
        "headphones", "webcam", "mechanical keyboard", "smartwatch",
    ],
}

# Default fallback
DEFAULT_COUNTRY = 'US'


def _get_queries(country):
    return COUNTRY_SEARCH_QUERIES.get(country, COUNTRY_SEARCH_QUERIES[DEFAULT_COUNTRY])

def _get_locations(country):
    return COUNTRY_MAP_LOCATIONS.get(country, COUNTRY_MAP_LOCATIONS[DEFAULT_COUNTRY])

def _get_yt_searches(country):
    return COUNTRY_YOUTUBE_SEARCHES.get(country, COUNTRY_YOUTUBE_SEARCHES[DEFAULT_COUNTRY])

def _get_shopping(country):
    return COUNTRY_SHOPPING_QUERIES.get(country, COUNTRY_SHOPPING_QUERIES[DEFAULT_COUNTRY])


# ─────────────────────────────────────────────────────────────────────────────
# Helper — human-like delays & scrolling
# ─────────────────────────────────────────────────────────────────────────────

async def _human_delay(min_s=1.5, max_s=5.0):
    """Random human-like pause."""
    await asyncio.sleep(random.uniform(min_s, max_s))


async def _human_scroll(page, scrolls=None):
    """Random scrolling down the page (1-5 scrolls)."""
    if scrolls is None:
        scrolls = random.randint(1, 5)
    for _ in range(scrolls):
        delta = random.randint(200, 600)
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(0.5, 2.0))


async def _human_scroll_up(page, scrolls=None):
    """Random scrolling up."""
    if scrolls is None:
        scrolls = random.randint(1, 3)
    for _ in range(scrolls):
        delta = random.randint(-600, -200)
        await page.mouse.wheel(0, delta)
        await asyncio.sleep(random.uniform(0.5, 1.5))


async def _safe_click(page, selector, timeout=5000):
    """Click element if visible, return True/False."""
    try:
        loc = page.locator(selector).first
        if await loc.count() > 0:
            await loc.wait_for(state='visible', timeout=timeout)
            await loc.click()
            return True
    except Exception:
        pass
    return False


async def _dismiss_google_consent(page):
    """Dismiss Google consent / cookie popup if present (optional, best-effort)."""
    try:
        # Selectors for 'Accept all' across languages (FR, EN, DE, etc.)
        selectors = [
            '#L2AGLb',                          # primary ID
            'button.tHlp8d',                    # class-based
            'button[aria-label*="Accept"]',     # EN aria
            'button[aria-label*="Accepter"]',   # FR aria
            'form[action*="consent"] button[value="1"]',  # consent form
        ]
        for sel in selectors:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                try:
                    await loc.click(timeout=3000)
                    await page.wait_for_load_state('domcontentloaded', timeout=5000)
                    return
                except Exception:
                    continue
    except Exception:
        pass


async def _safe_goto(page, url, worker_id, label=""):
    """Navigate with error handling."""
    try:
        await robust_goto(page, url, worker_id)
        await _human_delay(1.5, 3.0)
        await _dismiss_google_consent(page)
        await _human_delay(0.5, 1.5)
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# GMB-aware query generation helper
# ─────────────────────────────────────────────────────────────────────────────

def _gmb_search_queries(gmb_name, gmb_address):
    """Generate search queries that relate to the GMB business/location."""
    name = gmb_name or ''
    addr = gmb_address or ''
    # Extract city/area from address (last meaningful comma-part)
    addr_parts = [p.strip() for p in addr.split(',') if p.strip()]
    city = addr_parts[-1] if addr_parts else ''
    street = addr_parts[0] if addr_parts else ''
    first_word = name.split()[0] if name.split() else 'business'

    templates = [
        f"{name} reviews", f"{name} opening hours", f"{name} menu",
        f"{name} photos", f"{name} contact number", f"{name} prices",
        f"{name} directions", f"how to get to {name}", f"{name} parking",
        f"{name} near me", f"is {name} open today", f"{name} delivery",
        f"{name} reservations", f"{name} booking", f"{name} ratings",
        f"best {first_word} {city}", f"{first_word} near {street}",
        f"things to do near {name}", f"restaurants near {street} {city}",
        f"shops near {street} {city}", f"parking near {name}",
        f"hotels near {name}", f"cafes near {street}",
        f"what to eat near {street} {city}", f"events near {city}",
    ]
    return [q.strip() for q in templates if q.strip()]


def _gmb_maps_queries(gmb_name, gmb_address):
    """Generate Maps queries that relate to the GMB business/location."""
    name = gmb_name or ''
    addr = gmb_address or ''
    addr_parts = [p.strip() for p in addr.split(',') if p.strip()]
    city = addr_parts[-1] if addr_parts else ''
    street = addr_parts[0] if addr_parts else ''
    first_word = name.split()[0] if name.split() else 'business'

    templates = [
        name, f"{name} {city}", f"{name} {addr}",
        f"restaurants near {street} {city}", f"cafes near {street} {city}",
        f"parking near {name}", f"hotels near {name}",
        f"pharmacies near {street} {city}", f"gas stations near {street}",
        f"supermarket near {street} {city}", f"bank near {street}",
        f"ATM near {name}", f"gym near {street} {city}",
        f"{first_word} near {city}", f"things to do near {name}",
        f"shops near {street} {city}", f"parks near {name}",
    ]
    return [q.strip() for q in templates if q.strip()]


def _gmb_youtube_queries(gmb_name, gmb_address):
    """Generate YouTube queries that relate to the GMB business/area."""
    name = gmb_name or ''
    addr = gmb_address or ''
    addr_parts = [p.strip() for p in addr.split(',') if p.strip()]
    city = addr_parts[-1] if addr_parts else ''
    first_word = name.split()[0] if name.split() else 'food'

    templates = [
        f"{first_word} making video", f"best {first_word} {city}",
        f"{city} food vlog", f"{city} travel vlog", f"things to do {city}",
        f"{city} city walk", f"best restaurants {city}", f"{first_word} tutorial",
        f"{city} nightlife", f"{first_word} review taste test",
        f"visit {city} travel guide", f"{city} street food tour",
    ]
    return [q.strip() for q in templates if q.strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google Search
# ─────────────────────────────────────────────────────────────────────────────

async def _google_search(page, worker_id, country='US', query=None,
                         gmb_name='', gmb_address=''):
    """Perform a Google search, click 1-2 results, browse briefly."""
    if query is None:
        # If GMB info provided, sometimes (30%) use a GMB-related query
        if gmb_name and random.random() < 0.30:
            query = random.choice(_gmb_search_queries(gmb_name, gmb_address))
        else:
            queries = _get_queries(country)
            query = random.choice(queries)
    _log(worker_id, f"[HEALTH] Google Search: '{query}'")

    if not await _safe_goto(page, "https://www.google.com", worker_id):
        return

    # Dismiss consent popup if it appeared after page load
    await _dismiss_google_consent(page)

    # Type search query
    try:
        search_input = page.locator('textarea[name="q"], input[name="q"]').first
        if await search_input.count() > 0:
            await search_input.click()
            await _human_delay(0.5, 1.5)
            await search_input.fill(query)
            await _human_delay(0.5, 1.0)
            await page.keyboard.press('Enter')
            await _human_delay(2.0, 4.0)
        else:
            return
    except Exception:
        return

    # Scroll through results
    await _human_scroll(page, random.randint(1, 3))
    await _human_delay(1.0, 3.0)

    # Click 1-2 search results
    clicks = random.randint(1, 2)
    for _ in range(clicks):
        try:
            results = page.locator('div#search a h3')
            count = await results.count()
            if count > 0:
                idx = random.randint(0, min(count - 1, 7))
                await results.nth(idx).click()
                await _human_delay(3.0, 8.0)

                # Browse the page
                await _human_scroll(page, random.randint(1, 4))
                await _human_delay(2.0, 5.0)

                if random.random() > 0.5:
                    await _human_scroll_up(page, 1)
                    await _human_delay(1.0, 2.0)

                await page.go_back()
                await _human_delay(2.0, 4.0)
        except Exception:
            break

    _log(worker_id, f"[HEALTH] Google Search complete: '{query}'")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google Maps
# ─────────────────────────────────────────────────────────────────────────────

async def _browse_maps(page, worker_id, country='US', query=None,
                       gmb_name='', gmb_address=''):
    """Browse Google Maps — search a location, explore."""
    if query is not None:
        location = query
    elif gmb_name and random.random() < 0.30:
        location = random.choice(_gmb_maps_queries(gmb_name, gmb_address))
    else:
        locations = _get_locations(country)
        location = random.choice(locations)
    _log(worker_id, f"[HEALTH] Google Maps: '{location}'")

    if not await _safe_goto(page, "https://www.google.com/maps", worker_id):
        return

    await _human_delay(2.0, 4.0)

    try:
        search_box = page.locator('#searchboxinput').first
        if await search_box.count() > 0:
            await search_box.click()
            await _human_delay(0.5, 1.0)
            await search_box.fill(location)
            await _human_delay(0.5, 1.0)
            await page.keyboard.press('Enter')
            await _human_delay(3.0, 6.0)
        else:
            return
    except Exception:
        return

    await _human_scroll(page, random.randint(1, 3))
    await _human_delay(2.0, 5.0)

    # Try clicking on a nearby place
    try:
        place_links = page.locator('a[href*="/maps/place/"]')
        cnt = await place_links.count()
        if cnt > 0:
            idx = random.randint(0, min(cnt - 1, 4))
            await place_links.nth(idx).click()
            await _human_delay(3.0, 6.0)
            await _human_scroll(page, random.randint(1, 2))
    except Exception:
        pass

    # Zoom in/out randomly
    try:
        for _ in range(random.randint(1, 3)):
            key = random.choice(['+', '-'])
            await page.keyboard.press(key)
            await _human_delay(1.0, 2.0)
    except Exception:
        pass

    await _human_delay(2.0, 4.0)
    _log(worker_id, f"[HEALTH] Google Maps complete: '{location}'")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: YouTube
# ─────────────────────────────────────────────────────────────────────────────

async def _browse_youtube(page, worker_id, country='US', query=None,
                          gmb_name='', gmb_address=''):
    """Browse YouTube — search, click a video, watch briefly."""
    if query is None:
        if gmb_name and random.random() < 0.25:
            query = random.choice(_gmb_youtube_queries(gmb_name, gmb_address))
        else:
            yt_searches = _get_yt_searches(country)
            query = random.choice(yt_searches)
    _log(worker_id, f"[HEALTH] YouTube: '{query}'")

    if not await _safe_goto(page, "https://www.youtube.com", worker_id):
        return

    await _human_delay(2.0, 4.0)

    # Scroll the home feed first
    await _human_scroll(page, random.randint(1, 3))
    await _human_delay(1.0, 3.0)

    # Search
    try:
        search_input = page.locator('input#search, input[name="search_query"]').first
        if await search_input.count() > 0:
            await search_input.click()
            await _human_delay(0.5, 1.0)
            await search_input.fill(query)
            await _human_delay(0.5, 1.0)
            await page.keyboard.press('Enter')
            await _human_delay(3.0, 6.0)
        else:
            return
    except Exception:
        return

    await _human_scroll(page, random.randint(1, 3))
    await _human_delay(1.0, 2.0)

    # Click a video
    try:
        video_links = page.locator('a#video-title, ytd-video-renderer a#thumbnail')
        cnt = await video_links.count()
        if cnt > 0:
            idx = random.randint(0, min(cnt - 1, 5))
            await video_links.nth(idx).click()
            await _human_delay(3.0, 5.0)

            # "Watch" for 10-30 seconds
            watch_time = random.uniform(10, 30)
            _log(worker_id, f"[HEALTH] YouTube: watching video for {watch_time:.0f}s")
            await asyncio.sleep(watch_time)

            # Scroll down to comments area
            await _human_scroll(page, random.randint(1, 3))
            await _human_delay(2.0, 4.0)

            if random.random() > 0.5:
                await _human_scroll_up(page, 2)
                await _human_delay(1.0, 2.0)
    except Exception:
        pass

    _log(worker_id, f"[HEALTH] YouTube complete: '{query}'")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Gmail
# ─────────────────────────────────────────────────────────────────────────────

async def _check_gmail(page, worker_id, country='US', query=None,
                       gmb_name='', gmb_address=''):
    """Open Gmail inbox, scroll, maybe read an email."""
    _log(worker_id, "[HEALTH] Gmail: checking inbox")

    if not await _safe_goto(page, "https://mail.google.com/mail/u/0/#inbox", worker_id):
        return

    await _human_delay(3.0, 6.0)

    await _human_scroll(page, random.randint(1, 4))
    await _human_delay(2.0, 4.0)

    # Try clicking an email row
    try:
        email_rows = page.locator('tr.zA, div[role="row"]')
        cnt = await email_rows.count()
        if cnt > 0:
            idx = random.randint(0, min(cnt - 1, 9))
            await email_rows.nth(idx).click()
            await _human_delay(3.0, 6.0)

            await _human_scroll(page, random.randint(1, 3))
            await _human_delay(3.0, 6.0)

            await page.go_back()
            await _human_delay(2.0, 3.0)
    except Exception:
        pass

    # Maybe check another tab
    if random.random() > 0.6:
        try:
            tabs = page.locator('div[role="tab"]')
            cnt = await tabs.count()
            if cnt > 1:
                idx = random.randint(1, min(cnt - 1, 3))
                await tabs.nth(idx).click()
                await _human_delay(2.0, 4.0)
                await _human_scroll(page, random.randint(1, 2))
                await _human_delay(2.0, 3.0)
        except Exception:
            pass

    _log(worker_id, "[HEALTH] Gmail complete")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google Drive
# ─────────────────────────────────────────────────────────────────────────────

async def _visit_drive(page, worker_id, country='US', query=None,
                       gmb_name='', gmb_address=''):
    """Visit Google Drive, browse files."""
    _log(worker_id, "[HEALTH] Google Drive: browsing")

    if not await _safe_goto(page, "https://drive.google.com/drive/my-drive", worker_id):
        return

    await _human_delay(3.0, 5.0)

    await _human_scroll(page, random.randint(1, 3))
    await _human_delay(2.0, 4.0)

    try:
        items = page.locator('div[data-target="doc"]')
        cnt = await items.count()
        if cnt > 0:
            idx = random.randint(0, min(cnt - 1, 5))
            await items.nth(idx).click()
            await _human_delay(2.0, 4.0)
    except Exception:
        pass

    if random.random() > 0.5:
        try:
            await _safe_goto(page, "https://drive.google.com/drive/shared-with-me", worker_id)
            await _human_scroll(page, random.randint(1, 2))
            await _human_delay(2.0, 4.0)
        except Exception:
            pass

    _log(worker_id, "[HEALTH] Google Drive complete")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google Account
# ─────────────────────────────────────────────────────────────────────────────

async def _visit_account(page, worker_id, country='US', query=None,
                         gmb_name='', gmb_address=''):
    """Visit Google Account settings, click around."""
    _log(worker_id, "[HEALTH] Google Account: visiting settings")

    pages_to_visit = [
        "https://myaccount.google.com/",
        "https://myaccount.google.com/security",
        "https://myaccount.google.com/personal-info",
        "https://myaccount.google.com/data-and-privacy",
    ]

    visit_count = random.randint(1, 2)
    chosen = random.sample(pages_to_visit, min(visit_count, len(pages_to_visit)))

    for url in chosen:
        if not await _safe_goto(page, url, worker_id):
            continue

        await _human_delay(2.0, 4.0)
        await _human_scroll(page, random.randint(1, 3))
        await _human_delay(2.0, 5.0)

        if random.random() > 0.5:
            await _human_scroll_up(page, 1)
            await _human_delay(1.0, 2.0)

    _log(worker_id, "[HEALTH] Google Account complete")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google News
# ─────────────────────────────────────────────────────────────────────────────

async def _browse_news(page, worker_id, country='US', query=None,
                       gmb_name='', gmb_address=''):
    """Browse Google News — read headlines, click articles."""
    _log(worker_id, "[HEALTH] Google News: browsing")

    if not await _safe_goto(page, "https://news.google.com/", worker_id):
        return

    await _human_delay(2.0, 4.0)

    await _human_scroll(page, random.randint(2, 5))
    await _human_delay(2.0, 4.0)

    clicks = random.randint(1, 2)
    for _ in range(clicks):
        try:
            articles = page.locator('article a[href]')
            cnt = await articles.count()
            if cnt > 0:
                idx = random.randint(0, min(cnt - 1, 8))
                await articles.nth(idx).click()
                await _human_delay(4.0, 8.0)

                await _human_scroll(page, random.randint(2, 5))
                await _human_delay(3.0, 6.0)

                await page.go_back()
                await _human_delay(2.0, 3.0)
        except Exception:
            break

    _log(worker_id, "[HEALTH] Google News complete")


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Google Shopping
# ─────────────────────────────────────────────────────────────────────────────

async def _browse_shopping(page, worker_id, country='US', query=None,
                           gmb_name='', gmb_address=''):
    """Browse Google Shopping — search products, browse."""
    if query is None:
        shopping = _get_shopping(country)
        query = random.choice(shopping)
    _log(worker_id, f"[HEALTH] Google Shopping: '{query}'")

    url = f"https://www.google.com/search?q={query.replace(' ', '+')}&tbm=shop"
    if not await _safe_goto(page, url, worker_id):
        return

    await _human_delay(2.0, 4.0)

    await _human_scroll(page, random.randint(1, 4))
    await _human_delay(2.0, 4.0)

    try:
        products = page.locator('div.sh-dgr__gr-auto a')
        cnt = await products.count()
        if cnt > 0:
            idx = random.randint(0, min(cnt - 1, 5))
            await products.nth(idx).click()
            await _human_delay(3.0, 6.0)
            await _human_scroll(page, random.randint(1, 3))
            await _human_delay(2.0, 4.0)
            await page.go_back()
            await _human_delay(1.0, 2.0)
    except Exception:
        pass

    _log(worker_id, f"[HEALTH] Google Shopping complete: '{query}'")


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

ALL_ACTIVITIES = [
    _google_search,
    _browse_maps,
    _browse_youtube,
    _check_gmail,
    _visit_drive,
    _visit_account,
    _browse_news,
    _browse_shopping,
]


# ─────────────────────────────────────────────────────────────────────────────
# Simple new activity functions for additional Google services
# ─────────────────────────────────────────────────────────────────────────────

async def _visit_photos(page, worker_id, country='US', variant='',
                        query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://photos.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(200, 500))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_translate(page, worker_id, country='US', variant='',
                           query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://translate.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_calendar(page, worker_id, country='US', variant='',
                          query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://calendar.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_keep(page, worker_id, country='US', variant='',
                      query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://keep.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_play(page, worker_id, country='US', variant='',
                      query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://play.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(200, 500))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_meet(page, worker_id, country='US', variant='',
                      query=None, gmb_name='', gmb_address=''):
    try:
        await page.goto('https://meet.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_contacts(page, worker_id, country='US', variant='',
                          query=None, gmb_name='', gmb_address=''):
    """Visit Google Contacts."""
    try:
        await page.goto('https://contacts.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(200, 500))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_blogger(page, worker_id, country='US', variant='',
                         query=None, gmb_name='', gmb_address=''):
    """Visit Blogger."""
    try:
        await page.goto('https://www.blogger.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 400))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_sites(page, worker_id, country='US', variant='',
                       query=None, gmb_name='', gmb_address=''):
    """Visit Google Sites."""
    try:
        await page.goto('https://sites.google.com', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


async def _visit_forms(page, worker_id, country='US', variant='',
                       query=None, gmb_name='', gmb_address=''):
    """Visit Google Forms."""
    try:
        await page.goto('https://docs.google.com/forms', timeout=15000)
        await asyncio.sleep(random.uniform(2, 4))
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(random.uniform(1, 3))
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Activity: Custom GMB (Google My Business) browsing
# ─────────────────────────────────────────────────────────────────────────────

async def _custom_gmb_activity(page, worker_id, gmb_name='', gmb_address='', country='US'):
    """
    Simulate organic local search behaviour around a specific business.

    Steps:
      1. Google-search the business name, click results, browse.
      2. Search "[business name] [address]" on Google Maps.
      3. Search generic categories ("restaurants near [address]", etc.) on Maps.
      4. Browse nearby places on Maps.
      5. Run 5-8 varied search queries around the business location.
    """
    if not gmb_name:
        _log(worker_id, "[HEALTH] custom_gmb: no gmb_name provided, skipping")
        return

    address_part = gmb_address if gmb_address else ''
    _log(worker_id, f"[HEALTH] Custom GMB activity: '{gmb_name}' / '{address_part}'")

    # --- Step 1: Google-search the business name ---
    await _google_search(page, worker_id, country=country, query=gmb_name)
    await _human_delay(3.0, 6.0)

    # --- Step 2: Search business on Google Maps ---
    maps_query = f"{gmb_name} {address_part}".strip()
    _log(worker_id, f"[HEALTH] GMB Maps search: '{maps_query}'")
    if await _safe_goto(page, "https://www.google.com/maps", worker_id):
        try:
            search_box = page.locator('#searchboxinput').first
            if await search_box.count() > 0:
                await search_box.click()
                await _human_delay(0.5, 1.0)
                await search_box.fill(maps_query)
                await _human_delay(0.5, 1.0)
                await page.keyboard.press('Enter')
                await _human_delay(4.0, 8.0)
                await _human_scroll(page, random.randint(1, 3))
                await _human_delay(2.0, 4.0)
        except Exception:
            pass

    # --- Step 3: Search generic categories near the address ---
    if address_part:
        nearby_categories = ["restaurants", "shops", "cafes", "hotels", "pharmacies",
                             "gas stations", "banks", "gyms", "supermarkets", "parking"]
        chosen_cats = random.sample(nearby_categories, min(random.randint(2, 4), len(nearby_categories)))
        for cat in chosen_cats:
            cat_query = f"{cat} near {address_part}"
            _log(worker_id, f"[HEALTH] GMB nearby search: '{cat_query}'")
            if await _safe_goto(page, "https://www.google.com/maps", worker_id):
                try:
                    search_box = page.locator('#searchboxinput').first
                    if await search_box.count() > 0:
                        await search_box.click()
                        await _human_delay(0.5, 1.0)
                        await search_box.fill(cat_query)
                        await _human_delay(0.5, 1.0)
                        await page.keyboard.press('Enter')
                        await _human_delay(3.0, 6.0)
                        await _human_scroll(page, random.randint(1, 3))
                        await _human_delay(2.0, 4.0)

                        # Click a nearby place
                        try:
                            place_links = page.locator('a[href*="/maps/place/"]')
                            cnt = await place_links.count()
                            if cnt > 0:
                                idx = random.randint(0, min(cnt - 1, 4))
                                await place_links.nth(idx).click()
                                await _human_delay(3.0, 6.0)
                                await _human_scroll(page, random.randint(1, 2))
                        except Exception:
                            pass
                except Exception:
                    pass
            await _human_delay(2.0, 5.0)

    # --- Step 4: Varied search queries around the business ---
    query_templates = [
        f"{gmb_name} reviews",
        f"{gmb_name} opening hours",
        f"{gmb_name} contact",
        f"directions to {gmb_name}",
        f"{gmb_name} photos",
        f"best {gmb_name.split()[0] if gmb_name.split() else 'business'} near {address_part}",
        f"{gmb_name} menu" if random.random() > 0.5 else f"{gmb_name} services",
        f"things to do near {address_part}" if address_part else f"{gmb_name} location",
    ]
    num_queries = random.randint(5, 8)
    selected_queries = random.sample(query_templates, min(num_queries, len(query_templates)))
    for q in selected_queries:
        await _google_search(page, worker_id, country=country, query=q)
        await _human_delay(3.0, 8.0)

    _log(worker_id, f"[HEALTH] Custom GMB activity complete: '{gmb_name}'")


# ─────────────────────────────────────────────────────────────────────────────
# Activity ID → (function, query/variant) mapping
# ─────────────────────────────────────────────────────────────────────────────

SEARCH_QUERIES_BY_ID = {
    'search_restaurants': "best restaurants near me",
    'search_news': "latest news today",
    'search_weather': "weather forecast today",
    'search_movies': "best movies and shows 2025",
    'search_sports': "sports scores today",
    'search_tech': "technology news 2025",
    'search_travel': "best travel destinations 2025",
    'search_recipes': "easy dinner recipes",
    'search_products': "best product reviews 2025",
    'search_local': "local businesses near me",
}

# Activity-specific queries (used when a specific topic is requested)
# Falls back to random country pool when entry is missing.
# Each variant has 50-100 queries for maximum diversity.
ACTIVITY_QUERIES = {
    # ──────────────────────────────────────────────────────────────────
    # GOOGLE SEARCH variants (50-100 queries each)
    # ──────────────────────────────────────────────────────────────────
    'restaurants': [
        'best restaurants near me', 'restaurants open now', 'Italian restaurants nearby',
        'Chinese food delivery', 'best pizza places', 'sushi restaurant reviews',
        'Mexican restaurants near me', 'Thai food near me', 'Indian restaurants best rated',
        'steakhouse near me', 'seafood restaurants nearby', 'vegan restaurants near me',
        'brunch spots near me', 'fast food open now', 'fine dining near me',
        'cheap eats near me', 'family friendly restaurants', 'romantic dinner spots',
        'outdoor dining restaurants', 'best burger places near me', 'ramen shop near me',
        'Mediterranean restaurant reviews', 'Korean BBQ near me', 'Vietnamese pho restaurant',
        'French bistro near me', 'tapas restaurant nearby', 'breakfast restaurants near me',
        'late night food near me', 'buffet restaurants near me', 'farm to table restaurants',
        'food trucks near me', 'new restaurants recently opened', 'restaurant week deals',
        'best wings near me', 'BBQ restaurants nearby', 'vegetarian restaurants near me',
        'gluten free restaurants', 'halal restaurants near me', 'best tacos near me',
        'dim sum restaurant near me', 'Ethiopian restaurant nearby', 'Greek restaurant near me',
        'best salad places', 'Caribbean food near me', 'Peruvian restaurant nearby',
        'gastropub near me', 'soup restaurant near me', 'bakery cafe near me',
        'best dessert places near me', 'all you can eat restaurants', 'rooftop restaurants',
        'waterfront restaurants near me', 'sports bar with good food', 'best nachos near me',
        'restaurant happy hour deals', 'best fried chicken near me', 'pasta restaurants near me',
        'best dumplings near me', 'crab restaurant near me', 'lobster restaurant near me',
        'best brunch bottomless mimosas', 'trendy restaurants instagram', 'michelin star restaurants nearby',
    ],
    'news': [
        'breaking news today', 'latest world news', 'top headlines today',
        'CNN latest news', 'BBC news update', 'local news today',
        'US news headlines', 'politics news today', 'economy news latest',
        'technology news today', 'science news latest', 'health news today',
        'sports news headlines', 'entertainment news today', 'weather news alerts',
        'business news headlines', 'stock market news today', 'world events today',
        'climate change news latest', 'election news updates', 'crime news today',
        'education news latest', 'real estate news today', 'food industry news',
        'automotive news latest', 'travel news updates', 'celebrity news today',
        'space news latest', 'military news today', 'environmental news',
        'energy news updates', 'healthcare news today', 'AI news latest',
        'cryptocurrency news today', 'housing market news', 'job market news today',
        'supply chain news', 'inflation news latest', 'trade news updates',
        'pandemic news latest', 'natural disaster news today', 'aviation news',
        'shipping industry news', 'agriculture news today', 'water crisis news',
        'immigration news latest', 'gun control news', 'social media news today',
        'streaming service news', 'retail news latest', 'banking news today',
        'insurance industry news', 'startup news latest', 'venture capital news',
        'IPO news today', 'merger acquisition news', 'antitrust news',
        'labor union news', 'minimum wage news', 'remote work news today',
    ],
    'weather': [
        'weather forecast today', 'weather this week', 'weather tomorrow',
        'weather radar live', 'weather 10 day forecast', 'weather this weekend',
        'will it rain today', 'temperature today', 'humidity today',
        'UV index today', 'pollen count today', 'air quality index',
        'weather severe alerts', 'tornado watch today', 'hurricane tracker',
        'winter storm forecast', 'snow forecast this week', 'freeze warning tonight',
        'heat advisory today', 'wind speed today', 'sunset time today',
        'sunrise time tomorrow', 'weather for outdoor events', 'beach weather forecast',
        'hiking weather conditions', 'ski conditions forecast', 'boating weather',
        'weather for gardening today', 'allergy forecast today', 'fog advisory today',
        'thunderstorm forecast', 'hail forecast today', 'drought conditions',
        'rain probability today', 'dew point today', 'barometric pressure today',
        'weather camping this weekend', 'travel weather forecast', 'road conditions weather',
        'school delay weather', 'weather warnings my area', 'lightning tracker live',
        'flood warning today', 'wildfire smoke forecast', 'frost forecast tonight',
        'weather averages this month', 'coldest day this week', 'warmest day forecast',
        'weather compare cities', 'best weather today near me', 'outdoor event weather check',
    ],
    'movies': [
        'best movies 2025', 'new movies in theaters', 'top rated movies this year',
        'best movies on Netflix', 'new releases streaming', 'best action movies 2025',
        'horror movies new releases', 'comedy movies to watch', 'best drama movies',
        'animated movies for kids', 'best thriller movies', 'romantic movies 2025',
        'sci-fi movies new', 'documentary movies best', 'indie films 2025',
        'oscar nominated movies', 'best movies on Disney plus', 'HBO Max new movies',
        'Amazon Prime best movies', 'Apple TV plus movies', 'Hulu new releases',
        'foreign language films best', 'classic movies to watch', 'cult movies list',
        'movie reviews today', 'upcoming movie releases', 'movie showtimes near me',
        'IMAX movies playing now', 'drive in movie theaters', 'best movie soundtracks',
        'best sequels 2025', 'movies based on true stories', 'book adaptations movies',
        'superhero movies 2025', 'war movies best rated', 'sports movies inspirational',
        'Christmas movies best', 'Halloween movies list', 'summer blockbusters 2025',
        'best movies for date night', 'movies to watch with family', 'mind bending movies',
        'feel good movies list', 'best plot twist movies', 'underrated movies 2025',
        'box office results this week', 'highest grossing movies', 'movie marathon ideas',
        'best movie trilogies', 'movies leaving Netflix soon', 'best foreign films subtitled',
        'best cinematography movies', 'director filmography ranked', 'movie awards season',
    ],
    'sports': [
        'sports scores today', 'NFL scores live', 'NBA scores tonight',
        'MLB standings 2025', 'NHL scores today', 'premier league results',
        'Champions League scores', 'La Liga results today', 'Serie A standings',
        'Bundesliga scores', 'MLS scores today', 'college football scores',
        'college basketball scores', 'tennis ATP results', 'golf PGA scores',
        'Formula 1 results', 'NASCAR results today', 'boxing results last night',
        'UFC fight results', 'MMA news today', 'wrestling results',
        'cricket live scores', 'rugby scores today', 'volleyball results',
        'swimming world records', 'track and field results', 'cycling race results',
        'winter sports results', 'Olympic sports news', 'esports tournament results',
        'fantasy football rankings', 'sports betting lines', 'injury reports NFL',
        'trade rumors NBA', 'free agent signings', 'draft predictions 2025',
        'sports highlights today', 'game recap last night', 'player stats leaders',
        'sports schedule this week', 'tickets to games near me', 'sports bar near me',
        'soccer transfer news', 'best sports moments 2025', 'sports analysis today',
        'womens sports results', 'WNBA scores today', 'tennis WTA rankings',
        'sports podcast recommendations', 'live sports streaming free', 'sports app best',
        'gymnastics competition results', 'badminton tournament results', 'table tennis results',
    ],
    'tech': [
        'best smartphones 2025', 'latest technology news', 'best laptops 2025',
        'new iPhone features', 'Samsung Galaxy review', 'best budget phones',
        'tablet comparison 2025', 'smart watch reviews', 'best wireless earbuds',
        'gaming laptop reviews', 'best monitors for work', 'mechanical keyboard reviews',
        'best webcam for meetings', 'SSD comparison 2025', 'best graphics cards',
        'router wifi 6 review', 'smart home devices best', 'best VPN service review',
        'antivirus software comparison', 'cloud storage plans comparison', 'best printer reviews',
        'drone reviews 2025', 'e-reader comparison', 'projector for home theater',
        'best speakers bluetooth', 'noise cancelling headphones', 'USB-C hub reviews',
        'external hard drive best', 'power bank reviews', 'best fitness tracker',
        'robot vacuum reviews', 'best air purifier', 'smart doorbell camera',
        'best streaming device', 'Apple vs Android comparison', 'laptop vs tablet',
        'tech deals this week', 'refurbished electronics deals', 'tech gadgets under 50',
        'new tech products launching', 'CES best products', 'tech company earnings',
        'semiconductor industry news', 'quantum computing news', 'electric vehicle tech',
        'battery technology advances', '5G coverage map', 'fiber internet availability',
        'best password manager', 'two factor authentication apps', 'tech tips and tricks',
        'software updates today', 'best free software 2025', 'open source tools best',
    ],
    'travel': [
        'best travel destinations 2025', 'cheap flights deals today', 'travel packing list essentials',
        'best travel insurance review', 'solo travel tips safety', 'budget travel Europe',
        'best hotels near me', 'travel hacks save money', 'visa requirements by country',
        'best travel credit cards', 'beach destinations affordable', 'mountain vacations best',
        'cruise deals 2025', 'road trip planning tips', 'best cities to visit 2025',
        'all inclusive resorts deals', 'backpacking essentials list', 'luxury travel destinations',
        'adventure travel experiences', 'cultural travel destinations', 'eco tourism destinations',
        'best national parks to visit', 'train travel Europe pass', 'island vacations affordable',
        'honeymoon destinations best', 'family vacation ideas 2025', 'weekend getaway near me',
        'best hostels cities', 'Airbnb tips for guests', 'hotel booking best sites',
        'travel photography tips', 'best travel apps 2025', 'airport lounge access tips',
        'carry on packing tips', 'jet lag remedies', 'best travel adapters',
        'travel safety tips abroad', 'travel medical kit essentials', 'best travel pillow',
        'duty free shopping tips', 'best time to book flights', 'flight delay compensation',
        'best airline reviews', 'hidden gem destinations', 'off season travel deals',
        'digital nomad destinations', 'workation destinations best', 'slow travel tips',
        'travel loyalty programs', 'best travel gear 2025', 'travel journal ideas',
        'best food cities world', 'wine travel destinations', 'ski resort deals',
    ],
    'recipes': [
        'easy dinner recipes', 'quick lunch ideas', 'healthy breakfast recipes',
        'meal prep ideas weekly', 'air fryer recipes easy', 'slow cooker dump meals',
        'instant pot recipes beginner', 'sheet pan dinner recipes', 'one pot meals easy',
        'crockpot chicken recipes', 'vegetarian dinner ideas', 'vegan meal ideas simple',
        'keto diet recipes', 'low carb dinner recipes', 'gluten free recipes easy',
        'pasta recipes from scratch', 'homemade bread recipe easy', 'best cookie recipe ever',
        'chocolate cake from scratch', 'banana bread recipe moist', 'sourdough starter guide',
        'grilling recipes summer', 'smoked meat recipes', 'BBQ sauce homemade recipe',
        'salad recipes filling', 'soup recipes hearty', 'stew recipes winter',
        'casserole recipes easy', 'stirfry recipes quick', 'curry recipes simple',
        'taco recipes homemade', 'pizza dough recipe easy', 'sushi making at home',
        'ramen recipe authentic', 'fried rice recipe best', 'pad thai recipe simple',
        'breakfast burrito recipe', 'pancake recipe fluffy', 'french toast recipe',
        'smoothie bowl recipes', 'overnight oats recipes', 'energy balls recipe',
        'appetizer recipes party', 'dip recipes for chips', 'snack ideas healthy',
        'dessert recipes quick', 'ice cream recipe homemade', 'pie recipes from scratch',
        'holiday recipes traditional', 'potluck recipes crowd pleaser', 'budget meals family',
        'cooking for one recipes', 'kids lunch ideas easy', 'freezer meal recipes batch',
    ],
    'products': [
        'best product reviews 2025', 'product comparison website', 'consumer reports top rated',
        'best kitchen gadgets 2025', 'home appliance reviews', 'best vacuum cleaner 2025',
        'mattress reviews comparison', 'best office chair reviews', 'standing desk reviews',
        'best water filter pitcher', 'air fryer comparison chart', 'coffee maker reviews best',
        'blender comparison top rated', 'best food processor 2025', 'toaster oven reviews',
        'best lawn mower reviews', 'power tool reviews comparison', 'hand tool set reviews',
        'best garden hose', 'pressure washer reviews', 'best car seat safety rating',
        'stroller reviews comparison', 'baby monitor best rated', 'diaper brand comparison',
        'best luggage sets review', 'travel backpack reviews', 'duffel bag best rated',
        'best rain jacket review', 'hiking boots comparison', 'running shoe reviews 2025',
        'best sunglasses review', 'electric toothbrush comparison', 'best razor reviews',
        'skincare products top rated', 'best shampoo for hair type', 'perfume reviews popular',
        'best deodorant reviews', 'laundry detergent comparison', 'cleaning products best',
        'best dog food brands', 'cat litter comparison', 'pet bed reviews',
        'best noise machine sleep', 'pillow reviews side sleeper', 'weighted blanket reviews',
        'best bookshelf speakers', 'turntable reviews beginner', 'best board games review',
        'puzzle brand comparison', 'art supplies reviews', 'best journal notebooks',
        'pen reviews for writing', 'planner comparison 2025', 'best backpack everyday carry',
    ],
    'local': [
        'local businesses near me', 'things to do near me today', 'events near me this weekend',
        'local farmers market schedule', 'local craft fairs near me', 'community events today',
        'library hours near me', 'post office near me hours', 'DMV appointment near me',
        'car wash near me', 'dry cleaners near me', 'locksmith near me emergency',
        'plumber near me available today', 'electrician near me reviews', 'mechanic near me cheap',
        'dentist near me accepting patients', 'doctor near me walk in', 'urgent care near me open',
        'pharmacy near me 24 hour', 'vet clinic near me', 'pet grooming near me',
        'hair salon near me reviews', 'barber shop near me', 'nail salon near me',
        'spa near me deals', 'gym near me with pool', 'yoga studio near me',
        'dance classes near me', 'martial arts near me', 'swimming pool near me',
        'dog park near me', 'playground near me', 'hiking trails near me easy',
        'bike shop near me', 'thrift store near me', 'antique shops near me',
        'local bakery near me', 'ice cream shop near me', 'florist near me',
        'hardware store near me', 'garden center near me', 'auto parts store near me',
        'furniture store near me', 'mattress store near me', 'appliance repair near me',
        'tailoring alterations near me', 'shoe repair near me', 'picture framing near me',
        'storage units near me prices', 'moving company near me', 'recycling center near me',
        'donation center near me', 'food bank near me volunteer', 'tutoring center near me',
    ],
    'music': [
        'best music 2025', 'new album releases this week', 'top songs this week billboard',
        'best playlist for studying', 'chill music playlist', 'best jazz albums of all time',
        'indie music new releases', 'classical music for focus', 'best rap albums 2025',
        'how to read sheet music', 'music festivals near me 2025', 'concert tickets near me',
        'best vinyl records to own', 'music production for beginners', 'learn guitar online free',
        'learn piano for beginners', 'best music streaming service', 'spotify vs apple music',
        'best headphones for music', 'best portable speakers review', 'karaoke songs popular',
        'song lyrics search', 'music theory basics', 'best music documentaries',
        'live music venues near me', 'open mic nights near me', 'best music podcasts',
        'Grammy award winners 2025', 'best rock bands 2025', 'country music new releases',
        'R&B songs best 2025', 'electronic music festivals', 'lo-fi beats for relaxing',
        'workout music playlist energetic', 'road trip songs playlist', 'best 80s music hits',
        'best 90s music nostalgia', '2000s throwback songs', 'best movie soundtracks ever',
        'anime music playlist', 'video game music best', 'K-pop songs trending',
        'Latin music hits 2025', 'Afrobeats songs trending', 'reggaeton new releases',
        'best blues music albums', 'folk music recommendations', 'punk rock best bands',
        'metal music new releases', 'best love songs all time', 'best party songs playlist',
        'acoustic covers popular songs', 'best music videos 2025', 'music charts worldwide',
    ],
    'gardening': [
        'beginner gardening tips', 'how to grow tomatoes from seed', 'indoor herb garden setup',
        'composting for beginners guide', 'flower garden design ideas', 'raised bed garden plans diy',
        'best plants for shade garden', 'organic gardening tips beginners', 'vegetable garden layout planning',
        'when to plant seeds by zone', 'container gardening tips small spaces', 'succulent care guide indoor',
        'how to prune roses properly', 'lawn care schedule seasonal', 'best soil for raised beds',
        'garden pest control natural', 'companion planting guide chart', 'how to start a compost bin',
        'vertical garden ideas diy', 'hydroponic gardening at home', 'butterfly garden plants list',
        'drought tolerant landscaping ideas', 'how to grow peppers', 'herb garden kitchen windowsill',
        'perennial flower garden planning', 'annual flower planting guide', 'tree planting tips yard',
        'fruit tree care for beginners', 'berry bushes for backyard', 'grape vine growing guide',
        'garden tool essentials list', 'best garden hose review', 'drip irrigation setup garden',
        'mulching tips for garden beds', 'weed prevention garden beds', 'garden fence ideas cheap',
        'garden path ideas materials', 'outdoor lighting garden ideas', 'water feature garden small',
        'rain garden design guide', 'native plants for garden', 'pollinator garden planning',
        'winter garden preparation tips', 'seed starting indoors guide', 'greenhouse gardening basics',
        'square foot gardening method', 'permaculture garden design', 'food forest backyard',
        'growing mushrooms at home', 'microgreens growing guide', 'sprouts growing at home',
    ],
    'photography': [
        'photography tips for beginners', 'best camera 2025 review', 'portrait photography techniques',
        'landscape photography settings guide', 'photo editing software free best', 'smartphone photography tips tricks',
        'night photography settings', 'composition rules photography', 'macro photography guide beginners',
        'best lenses for portrait photography', 'street photography tips', 'wildlife photography gear',
        'wedding photography tips', 'product photography at home', 'food photography tips lighting',
        'real estate photography guide', 'astrophotography for beginners', 'drone photography tips',
        'black and white photography tips', 'golden hour photography tips', 'blue hour photography',
        'long exposure photography guide', 'HDR photography tutorial', 'panorama photography tips',
        'time lapse photography how to', 'underwater photography gear', 'film photography beginners',
        'instant camera reviews best', 'mirrorless vs DSLR 2025', 'best tripod for photography',
        'camera bag reviews best', 'SD card best for camera', 'photo backup solutions',
        'lightroom editing tutorial', 'photoshop basics beginners', 'free photo editing apps',
        'photo printing service best', 'photo book creation service', 'photography portfolio website',
        'photography business tips', 'stock photography selling guide', 'photography contest 2025',
        'best photography YouTube channels', 'photography inspiration ideas', 'photography challenge 30 days',
        'color theory photography', 'leading lines photography', 'symmetry in photography',
        'reflection photography ideas', 'silhouette photography tips', 'abstract photography ideas',
        'pet photography tips', 'newborn photography at home', 'family photo ideas outdoor',
    ],
    'astronomy': [
        'planets visible tonight', 'meteor shower schedule 2025', 'best telescope for beginners',
        'astronomy news today latest', 'star map tonight sky', 'how to see milky way tonight',
        'solar eclipse schedule 2025', 'James Webb telescope latest images', 'constellation guide beginners',
        'astrophotography for beginners guide', 'ISS tracker live position', 'moon phase tonight',
        'lunar eclipse 2025 dates', 'best binoculars for stargazing', 'dark sky locations near me',
        'astronomy apps best free', 'space station visible tonight', 'satellite tracker live',
        'solar system facts interesting', 'dwarf planets list facts', 'asteroid tracking NASA',
        'comet visible 2025', 'nebula photography beginner', 'galaxy types explained',
        'black hole latest discoveries', 'neutron star facts', 'supernova recent events',
        'exoplanet discoveries 2025', 'habitable zone planets list', 'alien life search news',
        'SETI project updates', 'radio telescope observations', 'gravitational waves detection',
        'dark matter explained simply', 'dark energy what is it', 'big bang theory explained',
        'universe age and size', 'multiverse theory explained', 'string theory basics',
        'Mars colonization plans', 'Moon base construction plans', 'space mining asteroids',
        'Voyager spacecraft update', 'Hubble telescope images best', 'JWST discoveries list',
        'SpaceX Starship updates', 'NASA Artemis program news', 'Blue Origin updates',
        'astronomy events this month', 'stargazing tips beginners', 'telescope eyepiece guide',
        'light pollution map', 'astronomy clubs near me', 'planetarium near me',
    ],
    'history': [
        'interesting history facts', 'ancient civilizations timeline', 'World War 2 key events',
        'history documentaries best rated', 'famous historical figures list', 'medieval history facts',
        'history of the internet timeline', 'ancient Egypt fascinating facts', 'Roman Empire history overview',
        'Cold War summary events', 'Renaissance period overview', 'Industrial Revolution impact',
        'American Civil War summary', 'French Revolution causes', 'history of democracy',
        'ancient Greek history facts', 'Viking history facts', 'Aztec civilization facts',
        'Mayan civilization discoveries', 'Inca Empire history', 'Silk Road history trade',
        'history of medicine timeline', 'history of aviation milestones', 'space race history timeline',
        'history of computing milestones', 'invention of printing press impact', 'history of photography',
        'history of music evolution', 'history of art movements', 'history of architecture styles',
        'ancient Rome daily life', 'medieval castle facts', 'samurai history Japan',
        'Ottoman Empire history', 'British Empire history', 'colonialism history impact',
        'history of slavery timeline', 'civil rights movement history', 'women suffrage movement',
        'history of religions overview', 'archaeological discoveries recent', 'lost civilizations mysteries',
        'famous battles in history', 'famous explorers list', 'history of espionage',
        'propaganda history examples', 'history of pandemics', 'great depression causes effects',
        'Berlin Wall history', 'apartheid South Africa history', 'history podcasts best rated',
        'history books must read', 'history museums best world', 'historical fiction books best',
    ],
    'science': [
        'science news today latest', 'interesting science experiments home', 'latest scientific discoveries 2025',
        'science fun facts amazing', 'quantum physics explained simply', 'climate science latest research',
        'biology fun facts nature', 'chemistry experiments safe home', 'space science discoveries',
        'science podcasts best rated', 'physics concepts explained easy', 'evolution theory explained',
        'genetics and DNA basics', 'neuroscience latest findings', 'artificial intelligence science',
        'robotics technology advances', 'nanotechnology applications', 'materials science breakthroughs',
        'renewable energy science', 'fusion energy research progress', 'solar panel technology new',
        'battery technology research', 'environmental science news', 'ocean science discoveries',
        'geology interesting facts', 'earthquake science prediction', 'volcano science news',
        'weather science meteorology', 'psychology research new findings', 'sociology studies interesting',
        'anthropology discoveries recent', 'paleontology dinosaur discoveries', 'fossil discoveries recent',
        'microbiology bacteria facts', 'virology research latest', 'immunology science news',
        'nutrition science latest', 'exercise science research', 'sleep science discoveries',
        'brain science latest research', 'consciousness science theories', 'memory research findings',
        'animal behavior science', 'plant science discoveries', 'marine biology facts',
        'ecology environmental science', 'biodiversity research 2025', 'conservation science news',
        'science fair project ideas', 'citizen science projects join', 'science YouTube channels best',
        'science books popular 2025', 'Nobel Prize science winners', 'science museums near me',
    ],
    'books': [
        'best books 2025 fiction', 'book recommendations mystery thriller', 'best self help books 2025',
        'new book releases this week', 'audiobook recommendations best', 'best mystery novels ever',
        'classic literature must read list', 'best non fiction books 2025', 'book club picks popular',
        'best fantasy series complete', 'science fiction books best', 'romance novels bestselling',
        'historical fiction books best', 'biography books inspiring', 'memoir books powerful',
        'business books must read', 'philosophy books beginners', 'psychology books popular',
        'poetry books best modern', 'graphic novels best rated', 'YA books trending 2025',
        'children books award winning', 'best debut novels 2025', 'literary fiction best',
        'horror books scariest ever', 'true crime books best', 'travel books inspiring',
        'cookbooks best sellers', 'art books coffee table', 'music books biographies',
        'sports books inspiring', 'political books 2025', 'economics books accessible',
        'science books popular science', 'nature books beautiful', 'gardening books best',
        'parenting books helpful', 'relationship books recommended', 'mindfulness books best',
        'productivity books top rated', 'leadership books must read', 'career books helpful',
        'writing books for aspiring authors', 'how to read more books tips', 'speed reading techniques',
        'best bookstores near me', 'library card benefits', 'ebook reader comparison',
        'Kindle vs Kobo comparison', 'best book subscription service', 'Goodreads alternatives',
        'book summary apps best', 'used books online cheap', 'rare books collecting guide',
    ],
    'art': [
        'art exhibitions near me current', 'famous paintings everyone should know', 'digital art for beginners tutorial',
        'art history timeline overview', 'how to draw portraits step by step', 'watercolor painting tips beginners',
        'modern art explained simply', 'virtual museum tours free', 'best art supplies for beginners',
        'street art famous cities', 'oil painting techniques beginner', 'acrylic painting tips',
        'sculpture art contemporary', 'art gallery near me', 'art classes near me adults',
        'art therapy benefits', 'abstract art how to create', 'pop art famous artists',
        'impressionism art movement', 'surrealism art explained', 'renaissance art masterpieces',
        'art deco design style', 'art nouveau characteristics', 'minimalism art movement',
        'art collecting for beginners', 'art prints affordable quality', 'framing art tips',
        'art competitions 2025', 'art festivals near me', 'art fairs upcoming',
        'digital illustration tools', 'procreate iPad art tutorial', 'Photoshop digital art',
        'AI art generation tools', 'NFT art marketplace', 'art career options',
        'art school programs best', 'art scholarship opportunities', 'art portfolio building tips',
        'art commission pricing guide', 'selling art online tips', 'Etsy art shop tips',
        'art journaling ideas', 'sketchbook drawing prompts', 'calligraphy for beginners',
        'pottery classes near me', 'ceramics for beginners', 'printmaking techniques',
        'textile art fiber art', 'glass art blowing near me', 'woodworking art projects',
        'art documentaries inspiring', 'art books best coffee table', 'art podcast recommendations',
    ],
    'cooking': [
        'cooking tips for beginners essential', 'easy recipes for weeknight dinner', 'how to meal prep efficiently',
        'baking tips and tricks for beginners', 'best cooking YouTube channels', 'kitchen gadgets actually worth buying',
        'how to cook steak perfectly medium rare', 'cooking with cast iron skillet', 'seasonal recipes spring ingredients',
        'one pot meals easy weeknight', 'knife skills basic techniques', 'how to make stock from scratch',
        'fermentation recipes at home', 'sourdough bread recipe beginner', 'homemade pasta from scratch',
        'wok cooking techniques', 'grilling tips for beginners', 'smoking meat at home tips',
        'sous vide cooking guide', 'pressure cooking instant pot tips', 'slow cooker recipes healthy',
        'air fryer best recipes', 'deep frying tips safety', 'sauteing techniques proper',
        'braising techniques meat', 'roasting vegetables tips', 'blanching technique how to',
        'poaching technique eggs fish', 'emulsification cooking science', 'caramelization cooking tips',
        'spice combinations chart', 'herb pairing guide food', 'seasoning food properly tips',
        'food plating presentation tips', 'cooking for a crowd tips', 'cooking on a budget meals',
        'pantry staples essential list', 'kitchen organization tips', 'food storage tips fresh',
        'freezing food guide properly', 'canning preserving food guide', 'pickling vegetables recipe',
        'cooking for picky eaters', 'cooking with kids recipes', 'date night cooking recipes',
        'international cuisine cooking basics', 'Thai cooking at home tips', 'Italian cooking authentic',
        'Mexican cooking traditional', 'Japanese cooking at home', 'Indian cooking spice guide',
        'French cooking techniques basic', 'Chinese cooking techniques wok', 'Korean cooking at home guide',
    ],
    'crypto': [
        'bitcoin price today live', 'ethereum price USD chart', 'crypto market cap total',
        'best cryptocurrency to buy 2025', 'bitcoin news today latest', 'solana price prediction analysis',
        'dogecoin news latest update', 'crypto portfolio tracker app', 'DeFi explained simply',
        'NFT market trends 2025', 'crypto exchange comparison best', 'how to buy bitcoin safely',
        'crypto wallet hardware best', 'altcoin season predictions', 'crypto mining profitable 2025',
        'blockchain technology explained', 'Web3 projects trending', 'DAO explained decentralized',
        'stablecoin comparison USDT USDC', 'crypto tax guide 2025', 'crypto regulation news',
        'Bitcoin ETF news latest', 'crypto staking rewards best', 'yield farming crypto guide',
        'crypto lending platforms comparison', 'layer 2 solutions crypto explained', 'zero knowledge proofs explained',
        'crypto security tips best practices', 'crypto scam prevention tips', 'rug pull crypto how to avoid',
        'metaverse crypto projects', 'gaming crypto tokens list', 'crypto airdrop upcoming free',
        'tokenomics explained simply', 'crypto whitepaper how to read', 'ICO vs IDO difference',
        'crypto market analysis today', 'bitcoin dominance chart', 'fear greed index crypto',
        'crypto trading strategies beginner', 'technical analysis crypto basics', 'DCA strategy crypto explained',
        'bitcoin halving impact price', 'ethereum merge update news', 'Cardano development updates',
        'Polkadot ecosystem projects', 'Avalanche network news', 'Chainlink oracle updates',
        'Cosmos ecosystem projects', 'XRP SEC lawsuit update', 'crypto adoption worldwide stats',
        'central bank digital currency news', 'crypto future predictions experts', 'crypto podcast best rated',
    ],
    'fashion': [
        'fashion trends 2025 spring summer', 'spring outfit ideas women', 'best fashion brands affordable',
        'street style inspiration looks', 'capsule wardrobe essentials list', 'affordable fashion haul ideas',
        'sustainable fashion brands list', 'how to style a blazer women', 'summer dress trends 2025',
        'men fashion trends 2025', 'workwear outfit ideas', 'casual friday outfit ideas',
        'date night outfit inspiration', 'festival outfit ideas', 'beach vacation outfits',
        'winter coat styles trending', 'layering clothes tips cold weather', 'athleisure outfit ideas',
        'vintage fashion style tips', 'thrift store fashion finds', 'how to build a wardrobe from scratch',
        'color matching outfits guide', 'pattern mixing fashion tips', 'accessorizing outfit tips',
        'best sneakers trending 2025', 'handbag trends 2025', 'jewelry trends current',
        'sunglasses styles trending', 'hat styles fashion guide', 'belt styling tips',
        'fashion week highlights 2025', 'designer fashion on a budget', 'fashion influencers to follow',
        'fashion apps for outfit ideas', 'clothing subscription box reviews', 'online clothing stores best',
        'fashion blog recommendations', 'fashion YouTube channels best', 'fashion podcast popular',
        'petite fashion tips', 'plus size fashion inspiration', 'tall women fashion tips',
        'fashion for different body types', 'dressing for your age tips', 'business casual guide',
        'smart casual outfit ideas', 'formal wear guide men women', 'wedding guest outfit ideas',
        'maternity fashion stylish', 'kids fashion trends', 'eco friendly clothing brands',
        'fabric types clothing guide', 'clothing care washing tips', 'closet organization ideas',
    ],
    'beauty': [
        'skincare routine morning step by step', 'best moisturizer for dry skin 2025', 'makeup tutorial beginners natural look',
        'hair care tips for hair growth', 'best beauty products 2025 awards', 'natural skincare routine organic',
        'anti aging serum best rated', 'eyeshadow blending techniques', 'curly hair care routine',
        'best drugstore makeup 2025', 'sunscreen best for daily wear', 'retinol skincare guide beginners',
        'vitamin C serum benefits skin', 'hyaluronic acid skincare use', 'niacinamide benefits for skin',
        'double cleansing method explained', 'clay mask benefits how often', 'exfoliating skincare guide',
        'lip care routine winter', 'nail care routine at home', 'eyebrow shaping tutorial',
        'contouring tutorial beginners', 'foundation matching guide skin tone', 'concealer application tips',
        'blush application techniques', 'highlighter makeup tutorial', 'setting spray vs powder',
        'false eyelash application tips', 'mascara best 2025 reviews', 'lipstick shades trending 2025',
        'hair coloring at home tips', 'best hair mask deep conditioning', 'heat protectant spray best',
        'hair straightening tips damage free', 'curling iron techniques tutorial', 'braiding hair tutorial easy',
        'updo hairstyle tutorial simple', 'hair growth supplements review', 'scalp care routine tips',
        'body lotion best moisturizing', 'body scrub homemade recipe', 'hand cream best for dry hands',
        'foot care routine at home', 'teeth whitening options safe', 'fragrance guide choosing perfume',
        'clean beauty brands list', 'K-beauty skincare routine', 'J-beauty products best',
        'men grooming tips skincare', 'beard grooming routine tips', 'dermatologist skincare advice',
        'acne treatment options best', 'dark circle remedies effective', 'beauty tools must have',
    ],
    'jobs': [
        'remote jobs work from home 2025', 'how to write cover letter example', 'job openings near me hiring now',
        'highest paying jobs without degree', 'LinkedIn profile optimization tips', 'resume writing tips 2025 best',
        'freelance jobs for beginners list', 'data entry jobs remote 2025', 'how to ace job interview tips',
        'side hustle ideas profitable 2025', 'career change advice 2025', 'salary negotiation tips',
        'best job search websites 2025', 'Indeed vs LinkedIn job search', 'Glassdoor company reviews',
        'internship opportunities 2025', 'entry level jobs near me', 'part time jobs flexible hours',
        'work from home equipment setup', 'remote work productivity tips', 'virtual assistant jobs',
        'customer service jobs remote', 'software developer jobs entry level', 'marketing jobs near me',
        'healthcare jobs in demand', 'teaching jobs online remote', 'accounting jobs near me',
        'project management jobs remote', 'sales jobs high commission', 'warehouse jobs near me hiring',
        'government jobs application guide', 'nonprofit jobs meaningful', 'startup jobs exciting',
        'gig economy jobs list', 'Uber Lyft driver tips', 'DoorDash delivery tips earnings',
        'Amazon warehouse jobs application', 'construction jobs near me', 'trade school careers high paying',
        'electrician apprenticeship near me', 'plumber career path guide', 'HVAC technician jobs',
        'truck driving jobs CDL', 'nursing jobs in demand 2025', 'pharmacy technician jobs',
        'dental hygienist career outlook', 'veterinary technician jobs', 'cybersecurity jobs entry level',
        'AI machine learning jobs 2025', 'UX design jobs remote', 'content writing jobs freelance',
        'photography jobs opportunities', 'real estate agent career start', 'personal trainer certification jobs',
    ],
    'finance': [
        'how to invest money beginners guide', 'stock market today live updates', 'best high yield savings account 2025',
        'credit score improvement tips fast', 'personal finance tips young adults', 'how to budget monthly effectively',
        'index fund vs ETF comparison', 'best dividend stocks 2025', 'how to get out of debt fast',
        'passive income ideas realistic 2025', 'Roth IRA vs traditional IRA', '401k contribution limits 2025',
        'compound interest calculator', 'emergency fund how much save', 'net worth calculator free',
        'tax filing tips maximize refund', 'tax deductions commonly missed', 'capital gains tax explanation',
        'real estate investing for beginners', 'REIT investing guide basics', 'bond investing explained',
        'mutual fund comparison tool', 'robo advisor comparison best', 'financial advisor worth it',
        'debt snowball vs avalanche method', 'student loan repayment strategies', 'mortgage refinance calculator',
        'home equity loan vs HELOC', 'insurance types explained guide', 'life insurance comparison term whole',
        'health insurance marketplace guide', 'car insurance comparison best rates', 'umbrella insurance explained',
        'social security benefits calculator', 'retirement planning timeline guide', 'FIRE movement explained',
        'cryptocurrency investing basics', 'gold investing pros cons', 'commodities investing guide',
        'forex trading for beginners', 'options trading explained simply', 'day trading tips beginners',
        'financial literacy resources free', 'money management apps best', 'budgeting apps comparison 2025',
        'best finance books 2025', 'finance podcasts popular', 'financial news today market',
        'inflation rate impact savings', 'interest rate forecast 2025', 'recession preparation tips',
    ],
    'fitness': [
        'best home workout routine no equipment', 'bodyweight exercises for beginners', 'how to lose belly fat exercises',
        'protein rich foods list complete', 'beginner yoga morning routine 20 min', 'gym workout plan beginners full body',
        'running tips for beginners couch to 5k', 'best pre workout foods natural', 'how to build muscle fast naturally',
        'intermittent fasting benefits weight loss', 'HIIT workout routine at home', 'strength training for beginners guide',
        'stretching routine full body', 'mobility exercises daily routine', 'core workout beginner routine',
        'upper body workout dumbbells', 'lower body workout at home', 'cardio exercises at home no equipment',
        'swimming workout plan beginner', 'cycling training plan beginner', 'jump rope workout routine',
        'resistance band exercises full body', 'kettlebell workout beginner routine', 'medicine ball exercises',
        'foam rolling routine recovery', 'post workout recovery tips', 'rest day importance fitness',
        'progressive overload explained fitness', 'muscle soreness remedies DOMS', 'hydration during exercise tips',
        'creatine supplement guide', 'protein powder comparison best', 'pre workout supplement review',
        'BCAA supplements worth it', 'electrolyte drinks comparison', 'fitness tracker comparison 2025',
        'best workout apps free 2025', 'personal trainer online vs gym', 'group fitness classes benefits',
        'CrossFit for beginners tips', 'Pilates vs yoga comparison', 'barre workout benefits',
        'calisthenics workout plan beginner', 'powerlifting program beginner', 'Olympic weightlifting basics',
        'marathon training plan beginner', 'trail running tips beginners', 'obstacle course race training',
        'workout motivation tips', 'fitness goal setting guide', 'body composition vs weight scale',
        'calorie counting basics guide', 'macro counting for beginners', 'meal plan for muscle gain',
    ],
    'health': [
        'symptoms of vitamin D deficiency', 'how to improve sleep quality tips', 'stress relief techniques quick',
        'best health supplements 2025', 'immune system boosting foods', 'mental health tips daily habits',
        'anxiety management techniques natural', 'healthy gut foods probiotics', 'hydration benefits health',
        'best vitamins for energy daily', 'blood pressure lowering naturally', 'cholesterol reducing foods',
        'diabetes prevention tips', 'heart health tips daily', 'back pain relief exercises',
        'headache remedies natural quick', 'allergy relief tips natural', 'cold flu remedies home',
        'sore throat remedies effective', 'digestive health improvement tips', 'joint pain relief natural',
        'eye health tips screen time', 'hearing health tips protection', 'dental health daily care',
        'skin health from inside', 'hair loss causes treatment', 'weight management healthy approach',
        'BMI calculator healthy range', 'blood sugar management tips', 'thyroid health natural support',
        'iron deficiency symptoms treatment', 'magnesium deficiency signs', 'vitamin B12 importance foods',
        'omega 3 benefits sources', 'antioxidant rich foods list', 'anti inflammatory diet foods',
        'Mediterranean diet benefits guide', 'plant based diet health benefits', 'intermittent fasting health effects',
        'mindfulness meditation health benefits', 'breathing exercises for health', 'cold shower benefits health',
        'sauna health benefits research', 'standing desk health benefits', 'walking daily health benefits',
        'sleep hygiene tips checklist', 'circadian rhythm optimization', 'melatonin natural production tips',
        'mental health apps best free', 'therapy types comparison guide', 'when to see a doctor symptoms',
        'preventive health screenings by age', 'vaccination schedule adults', 'health insurance explained simply',
    ],
    'realestate': [
        'houses for rent near me affordable', 'how to buy first home step by step', 'apartment rental tips guide',
        'real estate market trends 2025', 'best mortgage rates today comparison', 'how to negotiate house price tips',
        'home buying checklist complete', 'condo vs house pros cons comparison', 'investment property tips beginners',
        'rental property income guide', 'home appraisal process explained', 'home inspection checklist items',
        'closing costs breakdown explained', 'escrow process explained simply', 'title insurance explained',
        'property tax rates by area', 'homeowners insurance comparison', 'HOA fees pros cons',
        'home equity loan rates today', 'refinancing mortgage when worth it', 'mortgage pre approval process',
        'FHA loan requirements 2025', 'VA loan eligibility benefits', 'first time buyer programs 2025',
        'down payment assistance programs', 'rent vs buy calculator', 'housing affordability calculator',
        'real estate agent how to choose', 'FSBO selling home tips', 'home staging tips sell faster',
        'home renovation ROI best projects', 'kitchen remodel cost estimate', 'bathroom remodel ideas budget',
        'curb appeal improvement ideas', 'open house tips for buyers', 'real estate market forecast 2025',
        'commercial real estate investing', 'short term rental investment', 'REIT investing for income',
        'real estate crowdfunding platforms', 'land buying guide raw land', 'mobile home buying guide',
        'senior housing options guide', 'co-living spaces trend', 'tiny home communities near me',
        'new construction homes near me', 'foreclosure buying guide tips', 'auction properties how to buy',
        'property management tips landlord', 'tenant screening best practices', 'eviction process guide',
        'real estate apps best 2025', 'Zillow vs Redfin comparison', 'real estate podcasts best',
    ],
    'gaming': [
        'best games 2025 all platforms', 'PC gaming setup budget guide', 'best PS5 games 2025 list',
        'gaming news today latest', 'Minecraft survival tips advanced', 'best free PC games 2025',
        'gaming monitor review 2025', 'how to reduce lag online gaming', 'best gaming headset 2025',
        'Elden Ring tips for beginners', 'best Xbox games 2025', 'Nintendo Switch games best',
        'Steam Deck games recommended', 'gaming chair reviews comparison', 'gaming keyboard best 2025',
        'gaming mouse comparison review', 'best gaming controllers', 'game streaming setup guide',
        'Twitch streaming tips beginners', 'YouTube gaming channel tips', 'esports tournaments schedule',
        'competitive gaming tips improve', 'speedrunning community guide', 'retro gaming collection tips',
        'emulation guide legal options', 'game preservation history', 'indie games best 2025',
        'roguelike games best list', 'open world games 2025', 'RPG games best story',
        'FPS games competitive best', 'strategy games PC best', 'simulation games relaxing',
        'puzzle games brain teaser', 'horror games scariest 2025', 'couch co-op games best',
        'online multiplayer games free', 'MMO games popular 2025', 'mobile games best 2025',
        'VR games best experience', 'game deals PC sales today', 'gaming subscription comparison',
        'Game Pass vs PS Plus', 'game development for beginners', 'Unity game engine tutorial',
        'Unreal Engine 5 tutorial', 'game design principles basics', 'gaming industry news trends',
        'upcoming game releases 2025', 'game awards winners 2025', 'gaming podcasts best',
    ],
    'diy': [
        'DIY home improvement ideas budget', 'how to paint walls perfectly even', 'furniture restoration tips beginner',
        'DIY garden raised bed build', 'budget home decor ideas creative', 'how to fix squeaky floors quickly',
        'bathroom renovation tips budget', 'kitchen cabinet painting tutorial', 'outdoor patio ideas DIY',
        'DIY bookshelf plans easy', 'wallpaper installation tips DIY', 'tile installation bathroom floor',
        'hardwood floor refinishing DIY', 'drywall repair tutorial easy', 'caulking tips bathroom kitchen',
        'plumbing basics homeowner', 'electrical outlet installation safety', 'light fixture replacement DIY',
        'ceiling fan installation guide', 'smart thermostat installation', 'weatherstripping doors windows DIY',
        'insulation attic DIY tips', 'gutter cleaning maintenance tips', 'roof repair minor DIY',
        'fence building DIY wood', 'deck staining tips proper', 'concrete patio pour DIY',
        'shed building plans simple', 'garage organization ideas DIY', 'closet organizer build DIY',
        'floating shelves DIY plans', 'built in bookcase plans', 'window seat build tutorial',
        'headboard DIY ideas easy', 'coffee table build plans', 'workbench plans garage DIY',
        'kids playhouse build plans', 'tree house build safely', 'fire pit build outdoor DIY',
        'pergola build plans backyard', 'raised garden bed plans DIY', 'compost bin build easy',
        'chicken coop build plans', 'birdhouse build simple plans', 'picture frame build custom',
        'candle making at home DIY', 'soap making tutorial beginners', 'home fragrance DIY natural',
        'upholstery basics furniture DIY', 'curtain making sewing DIY', 'macrame wall hanging tutorial',
    ],
    'cars': [
        'best cars 2025 review comparison', 'used car buying checklist complete', 'car maintenance schedule guide',
        'electric car comparison 2025 best', 'best car insurance affordable rates', 'how to detail a car at home',
        'car tires when to replace signs', 'best SUV 2025 family', 'hybrid car benefits comparison',
        'car loan tips best rate', 'new car vs used car pros cons', 'car depreciation how it works',
        'test drive tips what to check', 'car warranty explained types', 'certified pre-owned cars worth it',
        'car safety ratings NHTSA 2025', 'best fuel efficient cars 2025', 'car reliability ratings brand',
        'car recall check by VIN', 'car registration renewal how to', 'emissions test requirements',
        'car wash tips proper technique', 'wax car properly tutorial', 'car interior cleaning tips',
        'windshield chip repair DIY', 'car battery replacement guide', 'jumper cables how to use',
        'flat tire change step by step', 'brake pad replacement signs', 'oil change how often needed',
        'coolant flush when to do', 'transmission fluid check change', 'air filter replacement car',
        'spark plug replacement guide', 'check engine light meaning common', 'car AC not cold fixes',
        'car dashboard warning lights guide', 'winter driving safety tips', 'driving in rain safety tips',
        'road trip car preparation', 'car emergency kit essentials', 'best dashcam 2025 review',
        'car phone mount best 2025', 'car seat covers best rated', 'floor mats car best',
        'best car GPS tracker', 'car theft prevention tips', 'parking tips parallel parking',
        'fuel saving driving tips', 'EV charging station map', 'electric car home charging setup',
        'car lease vs buy comparison', 'car trade in value maximize', 'car donation tax deduction',
    ],
    'pets': [
        'dog training tips at home effective', 'best dry cat food 2025 rated', 'pet care basics complete guide',
        'veterinarian near me ratings', 'dog breed comparison chart', 'puppy potty training schedule',
        'cat behavior understanding tips', 'best pet insurance 2025 comparison', 'fish tank setup beginner guide',
        'rabbit care tips indoor', 'hamster care guide complete', 'guinea pig care for beginners',
        'bird pet care basics', 'reptile pets for beginners', 'turtle care guide indoor',
        'dog food homemade recipe healthy', 'cat toys DIY ideas', 'dog walking tips new owner',
        'puppy socialization tips guide', 'dog crate training properly', 'leash training puppy tips',
        'dog separation anxiety solutions', 'cat scratching furniture solutions', 'dog barking control training',
        'pet dental care importance', 'flea tick prevention natural', 'heartworm prevention dogs guide',
        'pet vaccination schedule dogs cats', 'pet spaying neutering benefits', 'pet microchipping importance',
        'pet first aid kit essentials', 'dog park etiquette rules', 'cat litter box placement tips',
        'multi pet household tips harmony', 'introducing new pet to existing', 'pet adoption vs buying',
        'shelter pet adoption benefits', 'foster pet program volunteer', 'senior pet care special needs',
        'pet nutrition label reading guide', 'raw diet pets pros cons', 'grain free pet food debate',
        'pet weight management tips', 'exercise for dogs daily needs', 'cat exercise indoor ideas',
        'pet grooming at home tips', 'dog bathing frequency guide', 'cat grooming routine tips',
        'pet sitter vs boarding kennel', 'traveling with pets tips', 'pet friendly hotels near me',
        'emotional support animal guide', 'therapy dog training certification', 'service dog information',
    ],
    'education': [
        'online courses free with certificate', 'how to learn Python programming free', 'best books to read 2025',
        'study tips for exams effective', 'learn Spanish online free app', 'best learning apps 2025 rated',
        'how to speed read techniques', 'time management tips for students', 'best YouTube channels for learning',
        'Coursera vs Udemy comparison', 'Khan Academy courses free list', 'MIT OpenCourseWare best courses',
        'edX online courses popular', 'Skillshare free trial classes', 'LinkedIn Learning courses best',
        'coding bootcamp comparison 2025', 'data science course online best', 'web development course free',
        'graphic design course online', 'digital marketing course free', 'project management certification online',
        'writing course creative online', 'photography course online free', 'music theory course online',
        'language learning apps comparison', 'Duolingo vs Rosetta Stone', 'Babbel language learning review',
        'GED preparation online free', 'SAT prep free resources', 'GRE preparation tips and resources',
        'GMAT study plan guide', 'LSAT preparation course online', 'MCAT study schedule plan',
        'college admission essay tips', 'scholarship application tips guide', 'financial aid FAFSA guide',
        'study abroad programs benefits', 'gap year planning guide', 'career counseling resources free',
        'homeschooling resources curriculum', 'special education resources', 'STEM education activities kids',
        'critical thinking skills improve', 'public speaking skills course', 'debate skills learning',
        'research skills improvement tips', 'academic writing tips guide', 'citation format guide APA MLA',
        'note taking methods effective', 'mind mapping study technique', 'flashcard apps best for studying',
        'memory techniques memorization tips', 'concentration improvement tips', 'test anxiety management tips',
    ],
    'food': [
        'best restaurants near me open', 'easy dinner recipes tonight quick', 'healthy meal prep ideas weekly',
        'air fryer recipes easy quick', 'vegan recipes for beginners tasty', 'what to cook with chicken tonight',
        'quick breakfast ideas 10 minutes', 'baking tips for beginners guide', 'best pizza dough recipe homemade',
        'slow cooker recipes easy dump', 'food delivery near me best apps', 'meal kit delivery comparison',
        'grocery delivery service comparison', 'food blog recommendations popular', 'food Instagram accounts follow',
        'food photography tips smartphone', 'food truck festivals near me', 'food court best options',
        'organic food worth the price', 'farm to table restaurants near me', 'food waste reduction tips',
        'food preservation methods guide', 'canning food at home safely', 'dehydrating food at home',
        'fermented foods health benefits', 'kombucha making at home', 'kimchi recipe homemade easy',
        'sourdough bread making guide', 'cheese making at home beginner', 'chocolate making from beans',
        'coffee brewing methods comparison', 'tea varieties and benefits', 'smoothie recipes healthy breakfast',
        'juice cleanse recipes homemade', 'protein shake recipes post workout', 'cocktail recipes at home',
        'mocktail recipes non alcoholic', 'wine pairing guide food', 'beer pairing with food guide',
        'spice rack essentials list', 'pantry organization tips food', 'kitchen utensils essential list',
        'cookware comparison material guide', 'food scale best for cooking', 'food thermometer guide',
        'meal planning weekly template', 'budget grocery shopping tips', 'seasonal produce guide by month',
        'farmers market shopping tips', 'bulk buying food tips', 'food allergy safe cooking',
        'gluten free cooking tips', 'dairy free alternatives best', 'nut free recipes for allergies',
    ],
    'programming': [
        'programming for beginners where to start', 'best programming language to learn 2025', 'Python tutorial beginner free',
        'web development roadmap 2025', 'JavaScript projects for beginners', 'coding bootcamp reviews comparison',
        'GitHub trending projects today', 'data structures and algorithms guide', 'API development tutorial REST',
        'best code editor 2025 comparison', 'React tutorial for beginners', 'Node.js getting started guide',
        'TypeScript vs JavaScript comparison', 'SQL tutorial for beginners', 'database design basics guide',
        'Git version control tutorial', 'Docker tutorial for beginners', 'Kubernetes basics explained',
        'AWS getting started tutorial', 'cloud computing basics guide', 'DevOps roadmap 2025',
        'CI/CD pipeline setup guide', 'unit testing best practices', 'debugging techniques effective',
        'clean code principles guide', 'design patterns programming', 'SOLID principles explained',
        'microservices architecture explained', 'REST API best practices', 'GraphQL tutorial beginners',
        'machine learning Python tutorial', 'data science getting started', 'pandas tutorial data analysis',
        'web scraping Python tutorial', 'automation scripts Python beginner', 'command line basics tutorial',
        'Linux basics for developers', 'terminal commands essential list', 'shell scripting tutorial bash',
        'mobile app development getting started', 'Flutter tutorial for beginners', 'React Native tutorial 2025',
        'Swift iOS development tutorial', 'Android development Kotlin basics', 'game development Unity tutorial',
        'CSS layout techniques modern', 'responsive design tutorial', 'accessibility web development guide',
        'performance optimization web apps', 'security best practices web dev', 'SEO for developers basics',
        'open source contributing guide', 'technical interview preparation', 'coding challenges daily practice',
    ],
    'ai': [
        'artificial intelligence news 2025 latest', 'ChatGPT alternatives comparison', 'machine learning for beginners guide',
        'AI tools for productivity list', 'AI art generators best free', 'deep learning explained simply',
        'AI in healthcare applications', 'best AI apps 2025 useful', 'AI ethics debate current',
        'how to learn AI from scratch', 'AI writing assistants comparison', 'AI image generation tools',
        'AI video creation tools', 'AI music generation tools', 'AI coding assistants comparison',
        'AI chatbot comparison 2025', 'AI search engines new', 'AI summarization tools best',
        'AI translation tools accurate', 'AI voice synthesis tools', 'AI presentation maker tools',
        'AI spreadsheet tools analysis', 'AI email writing assistant', 'AI meeting notes tools',
        'AI customer service tools', 'AI marketing tools 2025', 'AI SEO tools comparison',
        'AI photo editing tools best', 'AI background remover tools', 'AI upscaling image tools',
        'large language models explained', 'GPT models comparison', 'Claude AI capabilities',
        'Gemini Google AI features', 'open source AI models list', 'AI fine tuning guide basics',
        'prompt engineering tips guide', 'AI automation workflows guide', 'AI agents explained concept',
        'multimodal AI explained', 'computer vision AI applications', 'natural language processing basics',
        'reinforcement learning explained', 'neural network basics guide', 'transformer architecture explained',
        'AI research papers recent', 'AI conferences 2025 schedule', 'AI career opportunities 2025',
        'AI certifications online courses', 'AI podcast recommendations best', 'AI newsletter subscriptions best',
        'AI regulation news worldwide', 'AI bias fairness issues', 'AI safety alignment research',
    ],
    'space': [
        'space news today latest discovery', 'NASA missions 2025 schedule', 'SpaceX launch schedule upcoming',
        'Mars exploration latest news', 'International Space Station updates', 'black hole discoveries recent',
        'exoplanets discovered habitable zone', 'space tourism 2025 flights', 'Artemis moon mission updates',
        'universe interesting facts mind blowing', 'rocket launch schedule this week', 'satellite internet Starlink updates',
        'space debris problem solutions', 'asteroid mining future plans', 'space station commercial private',
        'space suit technology advances', 'space food what astronauts eat', 'microgravity experiments research',
        'space telescope discoveries latest', 'cosmic rays research findings', 'solar wind effects Earth',
        'magnetosphere Earth protection', 'aurora borealis forecast tonight', 'space weather alerts today',
        'planetary defense asteroid redirect', 'lunar gateway station progress', 'Mars helicopter Ingenuity updates',
        'perseverance rover discoveries Mars', 'Europa mission Jupiter moon', 'Titan Saturn moon exploration',
        'Enceladus ocean life search', 'Venus missions planned future', 'Mercury mission BepiColombo',
        'Juno Jupiter mission findings', 'New Horizons Pluto beyond', 'interstellar probe concepts',
        'space elevator concept feasibility', 'O Neill cylinder space habitat', 'terraforming Mars feasibility',
        'Dyson sphere concept explained', 'Fermi paradox explanations', 'Drake equation variables',
        'SETI search progress updates', 'astrobiology research latest', 'extremophiles life forms Earth',
        'space law treaties regulations', 'space force military updates', 'space industry stocks invest',
        'space documentary recommendations', 'space books best 2025', 'space podcast popular',
    ],
    'environment': [
        'climate change solutions practical', 'renewable energy facts statistics', 'recycling tips at home easy',
        'sustainable living tips daily', 'electric vehicles comparison 2025', 'carbon footprint calculator personal',
        'plastic pollution solutions innovative', 'green energy sources overview', 'environmental news today latest',
        'how to reduce waste daily', 'solar panels home worth it', 'wind energy facts benefits',
        'geothermal energy explained', 'hydroelectric power advantages', 'nuclear energy debate pros cons',
        'hydrogen fuel cell cars', 'sustainable transportation options', 'public transit environmental benefits',
        'electric bike environmental impact', 'carbon offset programs comparison', 'tree planting organizations donate',
        'deforestation rate current statistics', 'reforestation projects worldwide', 'ocean cleanup projects progress',
        'coral reef restoration efforts', 'marine protected areas importance', 'endangered species list 2025',
        'wildlife conservation organizations donate', 'biodiversity loss impacts', 'invasive species problems solutions',
        'sustainable agriculture practices', 'organic farming benefits environment', 'regenerative farming explained',
        'food waste composting guide', 'zero waste lifestyle tips', 'minimalism environmental benefits',
        'fast fashion environmental impact', 'sustainable fashion alternatives', 'eco friendly products home',
        'green building standards LEED', 'energy efficient home tips', 'insulation home energy savings',
        'water conservation tips home', 'rainwater harvesting guide', 'drought resistant landscaping',
        'air quality monitoring local', 'indoor air quality improvement', 'environmental justice movement',
        'environmental policy news 2025', 'Paris Agreement progress update', 'COP climate conference news',
        'greenwashing how to spot', 'ESG investing explained guide', 'environmental career opportunities',
    ],
    'shopping': [
        'best deals Amazon today sale', 'online shopping tips save money', 'product comparison website best',
        'coupon codes active today', 'cashback shopping apps best', 'Best Buy deals today',
        'back to school shopping list', 'holiday gift ideas 2025', 'best subscription boxes review',
        'Black Friday deals early', 'Cyber Monday deals preview', 'Prime Day deals best',
        'price tracking tools browser', 'browser extension shopping deals', 'outlet store online deals',
        'warehouse club membership worth it', 'Costco best deals this week', 'Walmart deals online today',
        'Target circle deals today', 'thrift store shopping tips', 'consignment store near me',
        'garage sale tips buyer', 'estate sale near me today', 'flea market near me schedule',
        'refurbished electronics reliable', 'open box deals near me', 'clearance sale online stores',
        'student discount stores list', 'military discount retailers', 'senior discount shopping days',
        'loyalty programs best rewards', 'credit card rewards shopping', 'gift card deals discounts',
        'bulk buying tips save money', 'price match policy stores', 'return policy comparison stores',
        'online vs in store price comparison', 'shopping cart abandonment deals', 'wishlist price drop alerts',
        'seasonal sales calendar 2025', 'end of season sale dates', 'new product launch deals',
        'product review sites trustworthy', 'fake reviews how to spot', 'warranty extended worth it',
        'shipping free options retailers', 'international shipping shopping tips', 'package tracking tools best',
        'sustainable shopping guide tips', 'second hand marketplace apps', 'buy nothing groups local',
    ],
    'tech_news': [
        'technology news today breaking', 'new iPhone 2025 features release', 'best laptop 2025 comparison review',
        'artificial intelligence news latest', 'cybersecurity tips home protection', 'best smart home devices 2025',
        'Android vs iPhone 2025 comparison', 'best budget tablet 2025 review', 'cloud storage comparison plans',
        'best VPN service 2025 review', 'tech company earnings reports', 'semiconductor shortage update',
        'quantum computing breakthrough news', 'AR VR technology advances', '6G technology development news',
        'autonomous driving technology update', 'robotics industry news latest', 'wearable technology trends 2025',
        'foldable phone comparison 2025', 'satellite internet availability update', 'tech layoffs news updates',
        'tech startup funding news', 'app store changes policies', 'browser market share 2025',
        'social media platform changes', 'streaming service technology', 'gaming technology advances',
        'display technology OLED microLED', 'battery technology solid state', 'wireless charging technology',
        'USB 4 Thunderbolt comparison', 'WiFi 7 availability devices', 'Bluetooth latest version features',
        'smart glasses technology update', 'health tech wearables advances', 'fintech news latest updates',
        'edtech platform news updates', 'agritech innovations farming', 'biotech news latest discoveries',
        'cleantech renewable energy tech', 'spacetech startup news', 'proptech real estate tech',
        'legaltech AI in law', 'govtech government technology', 'martech marketing technology news',
        'insurtech innovations insurance', 'regtech compliance technology', 'tech podcast new episodes',
        'tech conference schedule 2025', 'tech book recommendations new', 'tech newsletter subscriptions best',
    ],
    'sports_news': [
        'sports news today headlines', 'Premier League results goals', 'NBA highlights today scores',
        'Formula 1 race results latest', 'tennis ATP rankings update', 'football transfer news rumors',
        'boxing results last night recap', 'cricket World Cup news', 'golf PGA Tour results leaderboard',
        'Olympic sports news medal count', 'NFL draft predictions analysis', 'MLB trade deadline rumors',
        'NHL playoff predictions analysis', 'soccer Champions League results', 'La Liga results standings',
        'Serie A results Italian football', 'Bundesliga results scores', 'rugby World Cup news',
        'volleyball nations league results', 'swimming championships results', 'track field world records',
        'cycling tour results stage', 'wrestling Olympic results', 'judo competition results',
        'fencing competition news', 'archery competition results', 'rowing regatta results',
        'sailing race results', 'equestrian competition news', 'skateboarding competition results',
        'surfing competition WSL results', 'climbing competition results', 'MMA UFC fight night results',
        'esports tournament results prize', 'fantasy sports tips weekly', 'sports betting odds today',
        'sports injury news updates', 'athlete endorsement news', 'sports contract negotiations news',
        'coaching changes news sports', 'sports venue construction news', 'youth sports development news',
        'women sports growth news', 'disability sports Paralympics news', 'sports technology innovation',
        'sports science research new', 'sports nutrition research', 'sports psychology insights',
        'sports history milestone today', 'sports documentary new release', 'sports podcast weekly episode',
    ],
    'news_world': [
        'world news today breaking headlines', 'breaking news live updates', 'economy news global 2025',
        'climate change news impact', 'political news today analysis', 'business news headlines markets',
        'science news 2025 breakthrough', 'space exploration news discovery', 'health news pandemic update',
        'environment news conservation', 'UN General Assembly news', 'G7 G20 summit news updates',
        'NATO alliance news updates', 'EU European Union news', 'BRICS nations news updates',
        'Middle East news latest', 'Asia Pacific news today', 'Africa news developments',
        'Latin America news today', 'Southeast Asia news updates', 'Eastern Europe news current',
        'trade agreements news global', 'sanctions news updates', 'diplomatic relations developments',
        'humanitarian crisis news updates', 'refugee migration news', 'global health WHO news',
        'pandemic preparedness news', 'food security global news', 'water crisis global news',
        'energy crisis news global', 'inflation rates worldwide', 'currency exchange rate news',
        'global stock markets today', 'commodity prices oil gold', 'shipping supply chain news',
        'cyber attack news global', 'espionage news intelligence', 'arms control treaty news',
        'nuclear policy news updates', 'space policy international news', 'ocean governance news',
        'Arctic region news developments', 'climate summit COP news', 'biodiversity conference news',
        'global education challenges news', 'technology regulation global news', 'AI governance international',
        'social media regulation global', 'press freedom index news', 'human rights news reports',
    ],
    'psychology': [
        'psychology facts interesting mind', 'how to improve mental health daily', 'cognitive behavioral therapy CBT basics',
        'psychology of habits formation', 'emotional intelligence improving tips', 'mindfulness meditation guide start',
        'psychology of persuasion principles', 'how to manage anxiety effectively', 'self improvement psychology tips',
        'body language reading tips guide', 'psychology of motivation factors', 'procrastination psychology overcoming',
        'imposter syndrome dealing with', 'perfectionism psychology managing', 'self esteem building exercises',
        'attachment theory relationships', 'love languages understanding types', 'communication skills psychology',
        'conflict resolution psychology tips', 'anger management techniques effective', 'forgiveness psychology benefits',
        'grief stages processing emotions', 'trauma recovery psychology guide', 'PTSD understanding symptoms',
        'depression signs and coping', 'bipolar disorder understanding', 'ADHD management strategies adults',
        'autism spectrum understanding', 'OCD management techniques', 'social anxiety coping strategies',
        'phobias common treatment options', 'personality types psychology', 'Myers Briggs personality test',
        'Big Five personality traits', 'narcissism psychology understanding', 'gaslighting recognizing signs',
        'toxic relationships psychology signs', 'boundaries setting psychology tips', 'people pleasing psychology overcoming',
        'decision making psychology tips', 'creativity psychology boosting', 'flow state psychology achieving',
        'positive psychology practices daily', 'gratitude psychology benefits', 'resilience building psychology',
        'growth mindset psychology developing', 'neuroplasticity brain training', 'sleep psychology importance',
        'dreams psychology meaning debate', 'color psychology marketing', 'consumer behavior psychology',
    ],
    'philosophy': [
        'philosophy for beginners introduction', 'famous philosophical quotes meaning', 'stoicism explained practical tips',
        'existentialism summary key ideas', 'best philosophy books accessible', 'philosophical questions thought provoking',
        'ethics philosophy basics overview', 'Socrates teachings key lessons', 'eastern philosophy introduction guide',
        'philosophy podcasts best rated', 'Plato Republic summary key points', 'Aristotle philosophy main ideas',
        'Nietzsche philosophy explained simply', 'Kant moral philosophy basics', 'utilitarianism explained pros cons',
        'free will determinism debate', 'consciousness philosophy debate', 'mind body problem philosophy',
        'epistemology what can we know', 'logic philosophy basics rules', 'critical thinking philosophy',
        'philosophy of science overview', 'philosophy of religion key debates', 'political philosophy main theories',
        'social contract theory explained', 'justice philosophy concepts', 'rights natural human debate',
        'environmental ethics philosophy', 'animal ethics philosophy debate', 'bioethics key issues',
        'technology ethics philosophy', 'AI ethics philosophical debate', 'privacy ethics digital age',
        'meaning of life philosophy views', 'absurdism Camus explained', 'nihilism explained responses to',
        'pragmatism philosophy approach', 'phenomenology philosophy explained', 'hermeneutics interpretation philosophy',
        'postmodernism philosophy explained', 'feminism philosophy overview', 'Marxism philosophy basics',
        'Buddhism philosophy key teachings', 'Taoism philosophy principles', 'Confucianism philosophy values',
        'Zen philosophy daily life', 'Hindu philosophy overview', 'Stoic daily practices modern',
        'philosophy of happiness overview', 'philosophy of love theories', 'philosophy of art aesthetics',
        'philosophy of education approaches', 'philosophy of language basics', 'philosophy of mathematics',
    ],
    'marketing': [
        'digital marketing tips 2025 strategy', 'social media marketing strategy guide', 'SEO tips for beginners 2025',
        'email marketing best practices guide', 'content marketing strategy guide', 'Google Ads tutorial beginners',
        'brand building strategies effective', 'influencer marketing tips guide', 'marketing analytics tools best',
        'copywriting tips persuasive writing', 'Facebook Ads tutorial guide', 'Instagram marketing strategy',
        'TikTok marketing for business', 'LinkedIn marketing B2B tips', 'YouTube marketing channel growth',
        'Pinterest marketing strategy tips', 'Twitter X marketing tips', 'podcast advertising effectiveness',
        'affiliate marketing for beginners', 'conversion rate optimization tips', 'landing page optimization guide',
        'A/B testing marketing guide', 'marketing funnel explained stages', 'customer journey mapping guide',
        'marketing automation tools comparison', 'CRM software comparison best', 'lead generation strategies',
        'cold outreach email templates', 'webinar marketing tips effective', 'event marketing strategy tips',
        'PR public relations tips brands', 'crisis communication marketing guide', 'reputation management online',
        'local SEO tips small business', 'Google My Business optimization', 'Yelp business management tips',
        'review management strategy online', 'user generated content marketing', 'video marketing strategy tips',
        'storytelling marketing brand narrative', 'data driven marketing approach', 'personalization marketing tips',
        'retargeting ads strategy guide', 'programmatic advertising explained', 'native advertising effectiveness',
        'marketing budget planning guide', 'marketing ROI measurement tools', 'KPI marketing metrics track',
        'marketing trends 2025 predictions', 'marketing certification online free', 'marketing podcast best episodes',
    ],
    'startups': [
        'how to start a startup guide', 'startup ideas 2025 profitable', 'venture capital explained process',
        'business plan template startup', 'startup funding stages explained', 'lean startup methodology guide',
        'startup success stories inspiring', 'how to pitch investors effectively', 'best startup books founders',
        'startup accelerator programs 2025', 'Y Combinator application tips', 'Techstars program review',
        'startup incubator vs accelerator', 'angel investor finding guide', 'seed funding how to raise',
        'Series A fundraising tips', 'startup valuation methods explained', 'equity cap table explained',
        'stock options startup employees', 'co-founder finding tips', 'startup team building advice',
        'MVP minimum viable product guide', 'product market fit finding', 'customer discovery interviews',
        'startup metrics to track', 'SaaS startup metrics guide', 'marketplace startup tips',
        'B2B startup strategies', 'B2C startup strategies tips', 'D2C brand building guide',
        'startup legal basics guide', 'startup incorporation state choice', 'startup intellectual property',
        'startup accounting basics', 'startup tax obligations guide', 'startup insurance needed types',
        'startup marketing bootstrap budget', 'growth hacking strategies startup', 'viral marketing techniques',
        'startup community networking tips', 'startup events conferences 2025', 'startup podcast founders',
        'startup newsletter subscriptions best', 'startup tools free essential', 'startup pitch deck template',
        'startup failure reasons common', 'pivot strategy startup when', 'startup exit strategies options',
        'acquisition startup preparation', 'IPO process startup steps', 'startup culture building tips',
    ],
    'parenting': [
        'parenting tips toddlers behavior', 'baby sleep schedule by age', 'kids activities at home rainy day',
        'healthy snacks for kids ideas', 'screen time guidelines children 2025', 'positive parenting techniques effective',
        'baby milestones month by month', 'kids education apps best free', 'family meal ideas everyone likes',
        'potty training tips 3 day method', 'newborn care tips first week', 'breastfeeding tips new mothers',
        'formula feeding guide comparison', 'baby led weaning guide start', 'infant sleep training methods',
        'toddler tantrums managing tips', 'preschool readiness skills checklist', 'kindergarten preparation activities',
        'homework help tips for parents', 'reading with kids tips benefits', 'math activities fun for kids',
        'science experiments kids safe', 'art projects kids easy', 'outdoor activities kids nature',
        'playdate organizing tips', 'birthday party ideas kids budget', 'summer camp options near me',
        'after school activities choosing', 'sports for kids by age', 'music lessons kids starting age',
        'sibling rivalry solutions tips', 'only child parenting considerations', 'blended family tips',
        'single parenting tips resources', 'co-parenting effectively guide', 'grandparent involvement tips',
        'child safety home proofing', 'car seat safety guidelines', 'water safety kids rules',
        'internet safety kids guide', 'bullying prevention tips parents', 'talking to kids about hard topics',
        'child mental health awareness', 'ADHD children parenting tips', 'autism support for parents',
        'gifted children nurturing talents', 'child development stages guide', 'teenager parenting tips',
        'college preparation timeline guide', 'financial literacy teaching kids', 'chore chart kids age appropriate',
    ],
    'architecture': [
        'famous architecture around the world', 'modern architecture trends 2025', 'sustainable architecture design green',
        'interior design ideas popular 2025', 'house design plans modern', 'architecture styles guide history',
        'tiny house designs functional', 'building materials guide comparison', 'gothic architecture history features',
        'famous architects and their buildings', 'brutalist architecture appreciation', 'art deco architecture cities',
        'mid century modern architecture', 'postmodern architecture examples', 'deconstructivism architecture buildings',
        'parametric architecture designs', 'biophilic design architecture nature', 'adaptive reuse architecture buildings',
        'passive house design principles', 'net zero building design', 'mass timber construction trend',
        '3D printed architecture buildings', 'modular construction prefab homes', 'floating architecture water homes',
        'underground architecture earth sheltered', 'treehouse architecture modern', 'skyscraper design tallest buildings',
        'bridge architecture famous designs', 'museum architecture iconic buildings', 'library architecture beautiful',
        'stadium architecture modern designs', 'airport architecture award winning', 'religious architecture worldwide',
        'residential architecture trends', 'commercial architecture office design', 'retail architecture store design',
        'landscape architecture garden design', 'urban planning city design', 'smart city architecture technology',
        'architectural visualization rendering', 'architectural photography tips', 'architectural drawing basics',
        'architecture software tools best', 'AutoCAD tutorial beginners', 'SketchUp architecture tutorial',
        'Revit BIM tutorial basics', 'architecture school programs ranked', 'architecture career paths options',
        'architecture books must read', 'architecture documentary films', 'architecture podcast recommendations',
    ],
    'interior_design': [
        'interior design ideas living room 2025', 'minimalist home decor tips', 'small apartment design ideas maximize space',
        'color schemes for rooms guide', 'furniture arrangement tips living room', 'home decoration trends 2025',
        'kitchen design ideas modern 2025', 'bathroom remodel ideas budget', 'bedroom design inspiration cozy',
        'home office design ideas productivity', 'open floor plan design tips', 'accent wall ideas paint',
        'wallpaper trends 2025 patterns', 'curtain window treatment ideas', 'rug placement guide rooms',
        'lighting design interior tips', 'pendant light placement guide', 'table lamp selection guide',
        'mirror placement interior design', 'art hanging arrangement wall', 'bookshelf styling decorating tips',
        'plant decoration indoor ideas', 'throw pillow mixing guide', 'blanket throw styling sofa',
        'coffee table styling decorating', 'dining table centerpiece ideas', 'kitchen countertop styling',
        'bathroom counter organization style', 'closet organization systems', 'entryway design ideas welcoming',
        'hallway decorating ideas narrow', 'staircase decorating ideas', 'basement finishing ideas design',
        'attic conversion design ideas', 'garage conversion living space', 'patio outdoor living design',
        'balcony small space design', 'Scandinavian interior design style', 'bohemian boho style decorating',
        'industrial style interior design', 'farmhouse style modern decorating', 'coastal interior design style',
        'mid century modern style decor', 'Japanese minimalist interior design', 'Mediterranean style decorating',
        'interior design on a budget tips', 'thrift store decor finds', 'DIY home decor projects easy',
        'interior design apps tools free', 'furniture shopping online best', 'interior design books inspiration',
    ],
    'politics': [
        'political news today analysis', 'election results latest updates', 'government policies explained simply',
        'how democracy works overview', 'political parties comparison platforms', 'local government news updates',
        'international relations news analysis', 'political debates schedule 2025', 'voter registration how to',
        'public policy changes impact', 'Congress legislation updates', 'Supreme Court decisions recent',
        'executive orders recent news', 'federal budget breakdown 2025', 'national debt explained impact',
        'tax policy changes 2025', 'healthcare policy debate current', 'education policy news updates',
        'immigration policy debate current', 'gun control policy debate', 'climate policy legislation',
        'infrastructure spending plans', 'social security policy changes', 'Medicare Medicaid policy news',
        'minimum wage debate current', 'labor policy union news', 'trade policy tariffs news',
        'foreign policy analysis current', 'defense spending budget debate', 'intelligence community news',
        'political polling latest numbers', 'campaign fundraising reports', 'political advertising analysis',
        'gerrymandering redistricting news', 'voting rights legislation', 'election security measures',
        'political fact checking resources', 'media bias analysis tools', 'political newsletter recommendations',
        'political podcast across spectrum', 'civic engagement opportunities', 'town hall meetings schedule',
        'petition government how to', 'contacting representatives guide', 'running for office guide',
        'political science basics overview', 'comparative politics countries', 'political theory introduction',
        'lobbying how it works', 'think tank organizations list', 'political satire commentary',
    ],
    'comedy': [
        'best comedy movies 2025 list', 'funny videos compilation today', 'stand up comedy specials new',
        'best comedy shows on Netflix', 'funny memes today trending', 'comedy podcasts best funniest',
        'sitcoms to binge watch 2025', 'clean funny jokes collection', 'comedy clubs near me tonight',
        'best comedians 2025 touring', 'comedy festival schedule 2025', 'improv comedy shows near me',
        'sketch comedy best shows', 'dark comedy movies best', 'romantic comedy movies new',
        'comedy animation shows adult', 'British comedy shows best', 'comedy writing tips beginners',
        'funny books humor fiction', 'comedy album recommendations', 'funny YouTube channels subscribe',
        'TikTok funny creators follow', 'Instagram comedy accounts best', 'Reddit funny subreddits best',
        'comedy roast best moments', 'comedy game shows watch', 'funny commercials compilation',
        'comedy variety shows best', 'late night show highlights', 'comedy news satire shows',
        'bloopers gag reel compilation', 'prank shows best moments', 'hidden camera shows funny',
        'comedy duo famous pairs', 'comedy legends classic performers', 'international comedy shows',
        'comedy in different languages', 'observational comedy best', 'physical comedy slapstick best',
        'absurdist comedy recommendations', 'parody movies best funniest', 'mockumentary shows best',
        'comedy trivia fun facts', 'funniest movie quotes ever', 'comedy awards history winners',
        'comedy writing books best', 'humor psychology why we laugh', 'office humor workplace comedy',
        'dad jokes best collection', 'puns wordplay best jokes', 'comedy open mic night tips',
    ],
    'podcasts': [
        'best podcasts 2025 all genres', 'true crime podcasts gripping', 'comedy podcasts funniest popular',
        'business podcasts top rated', 'science podcasts for curious minds', 'history podcasts engaging best',
        'how to start a podcast guide', 'podcast app comparison best', 'new podcast releases this week',
        'interview podcasts compelling top', 'technology podcasts insightful', 'health wellness podcasts inspiring',
        'self improvement podcasts motivating', 'parenting podcasts helpful', 'relationship advice podcasts',
        'sports podcasts entertaining analysis', 'music podcasts interesting stories', 'film movie podcasts review',
        'book club podcasts discussion', 'news analysis podcasts daily', 'political podcasts balanced views',
        'economics finance podcasts', 'entrepreneurship podcasts founders', 'marketing podcasts practical tips',
        'design podcasts creative inspiration', 'education podcasts learning', 'philosophy podcasts accessible',
        'psychology podcasts insights', 'spirituality meditation podcasts', 'food cooking podcasts entertaining',
        'travel podcasts adventure stories', 'nature environment podcasts', 'gaming podcasts industry news',
        'storytelling narrative podcasts', 'fiction audio drama podcasts', 'horror podcast scary stories',
        'podcast equipment recommendations', 'podcast hosting platforms comparison', 'podcast monetization tips',
        'podcast editing software best', 'podcast marketing tips grow', 'podcast guest booking tips',
        'podcast transcription services', 'podcast network joining benefits', 'podcast awards nominations 2025',
        'short podcasts under 20 minutes', 'long form podcasts deep dive', 'daily podcasts morning routine',
        'weekly podcasts worth following', 'podcast marathon binge listen', 'podcast recommendations by mood',
    ],
    'documentaries': [
        'best documentaries 2025 must watch', 'nature documentaries Netflix stunning', 'true crime documentaries gripping',
        'history documentaries educational best', 'science documentaries mind blowing', 'space documentaries awe inspiring',
        'food documentaries eye opening', 'social issue documentaries important', 'music documentaries inspiring',
        'ocean marine documentaries beautiful', 'war documentaries powerful', 'sports documentaries motivating',
        'technology documentaries insightful', 'art documentaries creative', 'travel documentaries wanderlust',
        'environmental documentaries urgent', 'political documentaries revealing', 'economic documentaries educational',
        'health medical documentaries', 'psychology mind documentaries', 'education system documentaries',
        'wildlife animal documentaries', 'deep sea documentaries exploration', 'mountain climbing documentaries',
        'cult sect documentaries chilling', 'conspiracy theory documentaries', 'unsolved mystery documentaries',
        'serial killer documentaries', 'heist robbery documentaries', 'undercover investigation documentaries',
        'celebrity biography documentaries', 'fashion industry documentaries', 'architecture design documentaries',
        'photography documentaries inspiring', 'journalism media documentaries', 'religious faith documentaries',
        'indigenous peoples documentaries', 'immigration documentaries stories', 'poverty inequality documentaries',
        'prison system documentaries', 'drug policy documentaries', 'mental health documentaries awareness',
        'disability documentaries empowering', 'LGBTQ documentaries important', 'feminist movement documentaries',
        'civil rights documentaries powerful', 'activism documentaries inspiring', 'innovation entrepreneurship documentaries',
        'AI technology future documentaries', 'space race documentaries classic', 'planet Earth documentary series',
    ],
    'anime': [
        'best anime 2025 season', 'anime recommendations action adventure', 'new anime releases schedule 2025',
        'anime streaming platforms comparison', 'top anime series all time ranked', 'anime movies highest rated',
        'slice of life anime relaxing', 'anime news today announcements', 'manga to anime adaptations 2025',
        'anime conventions 2025 schedule', 'isekai anime best recommendations', 'shonen anime top series',
        'seinen anime mature themes', 'shoujo anime romance best', 'mecha anime best series',
        'horror anime scariest shows', 'comedy anime funniest series', 'sports anime motivating shows',
        'music anime best series', 'food anime cooking shows', 'fantasy anime world building best',
        'sci fi anime futuristic best', 'historical anime period drama', 'psychological anime mind bending',
        'detective mystery anime best', 'romance anime heartwarming best', 'martial arts anime fighting',
        'supernatural anime powers best', 'school anime high school life', 'idol anime music performance',
        'anime art style appreciation', 'anime voice actors famous', 'anime soundtrack best OST',
        'anime figures collectibles', 'anime cosplay ideas popular', 'anime games best 2025',
        'anime merchandise where to buy', 'anime wallpaper desktop mobile', 'anime drawing tutorial style',
        'anime studio best productions', 'Studio Ghibli movies ranked', 'anime awards 2025 winners',
        'anime podcast recommendations', 'anime YouTube channels analysis', 'anime community forums best',
        'anime watch order guides series', 'anime filler guide skip list', 'anime recommendation quiz',
        'anime quotes inspiring famous', 'anime character analysis deep', 'anime tropes explained list',
    ],
    'manga': [
        'best manga 2025 series', 'manga recommendations all genres', 'new manga releases this week',
        'manga reading apps best', 'top manga series all time', 'shonen manga most popular',
        'manga art style tutorial drawing', 'manga vs anime comparison differences', 'seinen manga top rated mature',
        'manga apps official legal best', 'manga physical collection buying', 'manga box set deals',
        'manga new chapter releases weekly', 'manga author mangaka famous', 'manga awards 2025 winners',
        'romance manga best heartwarming', 'action manga exciting series', 'horror manga scariest titles',
        'comedy manga funniest series', 'fantasy manga world building best', 'sci fi manga futuristic',
        'sports manga motivating series', 'cooking food manga best', 'slice of life manga relaxing',
        'historical manga period setting', 'mystery detective manga best', 'psychological manga thriller',
        'music manga band stories', 'art drawing manga within manga', 'isekai manga best transport',
        'manhwa Korean manga recommendations', 'manhua Chinese manga recommendations', 'webtoon best series popular',
        'manga coloring techniques digital', 'manga panel layout guide', 'manga storytelling techniques',
        'manga industry Japan news', 'manga cafe experience Japan', 'manga museum exhibitions',
        'completed manga series binge read', 'ongoing manga series follow', 'manga adaptation live action',
        'manga to anime comparison quality', 'manga spin off series best', 'manga anthology collections',
        'manga for beginners easy read', 'manga in English translation', 'manga fan translation community',
        'manga collecting tips storage', 'rare manga valuable editions', 'manga convention events 2025',
    ],
    'board_games': [
        'best board games 2025 new releases', 'board games for 2 players fun', 'strategy board games complex',
        'family board games all ages fun', 'board game cafe near me', 'new board game releases 2025',
        'cooperative board games team play', 'party board games large groups', 'board game reviews trusted',
        'how to play Catan strategy tips', 'board game accessories upgrades', 'board game storage organization',
        'board game table recommendations', 'board game shelf display ideas', 'card games similar to board games',
        'dice games fun family', 'tile placement games best', 'worker placement games best',
        'deck building games recommendations', 'legacy board games best series', 'solo board games single player',
        'board games for kids educational', 'board games teenagers enjoy', 'board games adults dinner party',
        'board games couples date night', 'quick board games 30 minutes', 'epic board games long session',
        'board games with miniatures best', 'board games abstract strategy', 'word games board game style',
        'trivia board games fun', 'deduction games social mystery', 'bluffing games fun social',
        'area control board games best', 'engine building board games', 'auction bidding board games',
        'roll and write games best', 'push your luck games fun', 'dexterity board games skill',
        'board game Kickstarter upcoming', 'board game convention schedule 2025', 'board game podcast shows',
        'board game YouTube channels reviews', 'board game app digital versions', 'board game design creating',
        'board game history evolution', 'chess strategy tips improve', 'Go game strategy beginners',
        'Mahjong how to play rules', 'backgammon strategy guide', 'board game night hosting tips',
    ],
    'camping': [
        'camping essentials checklist complete', 'best camping spots near me reviews', 'camping gear reviews comparison 2025',
        'camping recipes easy campfire', 'tent camping tips beginners guide', 'camping with kids tips activities',
        'best sleeping bag 2025 review', 'camping cooking equipment essentials', 'national parks camping reservations',
        'winter camping tips cold weather', 'car camping setup comfortable', 'backpacking camping ultralight tips',
        'glamping luxury camping near me', 'hammock camping setup guide', 'camping lantern best review',
        'camping stove comparison best', 'camping water filter purifier', 'camping first aid kit essentials',
        'bear safety camping tips', 'wildlife safety camping rules', 'campfire building techniques proper',
        'leave no trace camping principles', 'camping knots essential ties', 'camping navigation compass GPS',
        'camping weather preparation tips', 'rain camping tips staying dry', 'hot weather camping keeping cool',
        'camping food storage bear canister', 'camping meal planning guide', 'dehydrated food camping meals',
        'camping coffee making methods', 'camping desserts smores recipes', 'camping snacks trail mix recipes',
        'camping games activities group', 'camping photography tips nature', 'stargazing camping best spots',
        'fishing while camping tips', 'kayaking camping trip planning', 'mountain biking camping trip',
        'beach camping tips oceanside', 'desert camping tips precautions', 'forest camping tips guide',
        'camping reservation tips peak season', 'free camping dispersed BLM land', 'RV camping tips beginners',
        'camping apps best planning', 'camping YouTube channels watch', 'camping gear deals sales',
        'camping chair best comfortable', 'camping tent comparison 2025', 'camping with pets dogs tips',
    ],
    'fishing': [
        'fishing tips for beginners complete', 'best fishing spots near me local', 'fishing gear essentials starter kit',
        'fly fishing guide getting started', 'bass fishing tips techniques', 'fishing license requirements by state',
        'best fishing rods 2025 review', 'fishing knots tutorial essential', 'saltwater fishing tips coastal',
        'ice fishing beginners guide tips', 'trout fishing tips stream', 'catfish fishing tips bait',
        'crappie fishing techniques tips', 'walleye fishing tips tricks', 'pike fishing techniques lures',
        'salmon fishing guide seasonal', 'carp fishing tips methods', 'surf fishing tips beach',
        'kayak fishing tips setup', 'boat fishing tips beginners', 'dock pier fishing tips',
        'night fishing tips techniques', 'fishing lure selection guide', 'live bait fishing guide',
        'fishing reel types comparison', 'spinning reel vs baitcaster', 'fishing line types guide',
        'fishing tackle organization tips', 'fish finder depth finder guide', 'fishing electronics technology',
        'fishing regulations local rules', 'catch and release tips proper', 'fish cleaning filleting guide',
        'fish cooking recipes fresh caught', 'fish smoking preserving guide', 'fishing tournament tips competing',
        'fishing photography tips catch', 'fishing apps best tracking', 'fishing weather conditions best',
        'fishing moon phase guide', 'tide chart fishing timing', 'fishing club joining local',
        'fishing charter booking tips', 'fishing vacation destinations best', 'fishing YouTube channels learn',
        'fishing podcast recommendations', 'fishing magazine online subscription', 'fishing gear maintenance care',
        'kid friendly fishing tips', 'fishing for relaxation stress', 'conservation fishing practices',
    ],
    'cycling': [
        'cycling for beginners getting started', 'best bikes 2025 review comparison', 'cycling routes near me scenic',
        'road bike vs mountain bike choosing', 'cycling gear essentials list', 'bike maintenance tips basic',
        'cycling training plan beginners', 'best cycling apps GPS tracking', 'electric bike reviews 2025',
        'cycling safety tips road rules', 'bike helmet reviews best 2025', 'cycling shorts padded review',
        'cycling jersey breathable best', 'cycling gloves review comparison', 'cycling shoes clipless guide',
        'bike lights best visibility safety', 'bike lock best theft prevention', 'bike pump portable best',
        'bike repair kit essentials', 'flat tire repair bike quick', 'bike chain maintenance cleaning',
        'bike brake adjustment guide', 'bike derailleur tuning guide', 'bike seat comfort adjustment',
        'cycling nutrition tips during ride', 'hydration cycling water bottles', 'cycling energy bars gels',
        'cycling stretches before after ride', 'cycling injuries prevention tips', 'cycling knee pain prevention',
        'indoor cycling trainer setup', 'spin class vs outdoor cycling', 'Peloton alternatives comparison',
        'cycling cadence training tips', 'hill climbing cycling technique', 'drafting cycling group ride',
        'century ride training plan', 'cycling race training beginner', 'triathlon cycling training tips',
        'bike commuting tips beginners', 'bike touring long distance tips', 'bikepacking adventure guide',
        'mountain biking trails near me', 'gravel biking getting started', 'BMX riding tips beginners',
        'cycling community group rides', 'cycling events races 2025', 'cycling vacation destinations',
        'bike fit professional adjustment', 'bike buying guide budget', 'used bike buying tips',
        'bike storage solutions home', 'bike rack car best review', 'cycling podcast recommendations',
    ],

    # ──────────────────────────────────────────────────────────────────
    # YOUTUBE variants (50 queries each)
    # ──────────────────────────────────────────────────────────────────
    'yt_feed': [
        'trending videos today', 'most popular videos right now', 'viral videos this week',
        'recommended videos YouTube', 'YouTube rewind best moments', 'satisfying videos compilation',
        'oddly satisfying videos watch', 'relaxing videos to unwind', 'ASMR videos popular',
        'time lapse videos amazing', 'compilation videos best of', 'behind the scenes videos',
        'unboxing videos popular products', 'reaction videos funny best', 'challenge videos trending',
        'prank videos harmless funny', 'day in the life vlog', 'morning routine video popular',
        'night routine relaxing video', 'room tour video popular', 'haul video shopping recent',
        'get ready with me video', 'what I eat in a day', 'grocery haul video weekly',
        'clean with me motivation', 'organize with me video', 'study with me video focus',
        'work with me productivity video', 'plan with me planner video', 'cook with me recipe video',
        'paint with me art video', 'build with me DIY video', 'garden with me outdoor video',
        'walk with me nature video', 'drive with me scenic video', 'fly with me travel video',
        'sunset video relaxing watch', 'city walk video exploring', 'drone footage amazing views',
        'cute animal videos compilation', 'puppy videos adorable', 'kitten videos playful funny',
        'baby videos cute moments', 'wedding videos emotional beautiful', 'surprise videos heartwarming',
        'transformation videos before after', 'speed art drawing time lapse', 'calligraphy video satisfying',
        'origami tutorial easy', 'magic trick videos impressive', 'science experiment videos cool',
    ],
    'yt_trending': [
        'YouTube trending videos today', 'most viewed video today', 'viral video of the week',
        'trending music video new', 'trending comedy video today', 'trending news video clip',
        'most liked video today YouTube', 'fastest growing YouTube channel', 'new YouTuber breakout viral',
        'celebrity YouTube video new', 'brand new music video premiere', 'movie trailer new release',
        'TV show clip trending', 'sports highlight viral play', 'gaming video trending new',
        'tech review trending product', 'fashion video trending style', 'beauty video trending tutorial',
        'food video trending recipe', 'travel video trending destination', 'fitness video trending workout',
        'education video trending topic', 'science video trending discovery', 'art video trending creation',
        'dance video trending choreography', 'singing video trending cover', 'comedy skit trending funny',
        'animation video trending new', 'documentary clip trending topic', 'podcast clip trending discussion',
        'interview clip trending celebrity', 'live stream trending event', 'shorts trending today YouTube',
        'meme video trending funny', 'parody video trending humorous', 'reaction video trending clip',
        'challenge video trending new', 'collab video trending YouTubers', 'community post trending YouTube',
        'YouTube premiere trending video', 'compilation video trending clips', 'top 10 video trending list',
        'review video trending product', 'tutorial video trending skill', 'story time video trending tale',
        'experiment video trending result', 'comparison video trending versus', 'debunk video trending myth',
        'how it works video trending', 'history video trending topic',
    ],
    'yt_music': [
        'music video new release today', 'top songs playlist 2025', 'chill music playlist study',
        'workout music playlist motivation', 'road trip music playlist', 'relaxing music sleep piano',
        'jazz music playlist smooth', 'classical music playlist focus', 'lo-fi beats study chill',
        'indie music playlist discover', 'pop music hits 2025', 'rock music playlist classic',
        'hip hop music new releases', 'R&B music smooth playlist', 'country music playlist hits',
        'electronic music playlist dance', 'reggaeton music playlist party', 'K-pop music trending hits',
        'Latin music playlist vibrant', 'Afrobeats music playlist groove', 'acoustic covers popular songs',
        'live performance concert video', 'unplugged session acoustic music', 'music festival highlights video',
        'karaoke songs popular with lyrics', 'guitar tutorial song learn', 'piano tutorial popular song',
        'drum cover popular song', 'bass guitar tutorial beginner', 'singing tutorial vocal tips',
        'music production tutorial beat', 'DJ mixing tutorial beginner', 'music theory lesson explained',
        'songwriting tips techniques', 'album review new release analysis', 'music history documentary',
        'artist biography music video', 'behind the song meaning', 'sample breakdown music production',
        'vinyl record collection tour', 'music gear review instrument', 'audio equipment review headphones',
        'Spotify vs Apple Music comparison', 'new music Friday playlist', 'throwback music 90s 2000s',
        'cover song best rendition', 'mashup music creative remix', 'one hit wonders music list',
        'music awards ceremony highlights', 'music reaction video first time', 'rare music find obscure',
    ],
    'yt_shorts': [
        'YouTube shorts funny today', 'shorts viral clips best', 'shorts satisfying compilation',
        'shorts life hacks quick', 'shorts cooking recipe 60 seconds', 'shorts animal cute clips',
        'shorts magic trick quick', 'shorts science fact mind blowing', 'shorts workout quick exercise',
        'shorts dance trend popular', 'shorts comedy skit short', 'shorts art speed draw',
        'shorts music cover clip', 'shorts fashion outfit idea', 'shorts beauty tip quick',
        'shorts tech tip useful', 'shorts travel clip beautiful', 'shorts sports highlight amazing',
        'shorts gaming clip epic', 'shorts food review bite', 'shorts motivation quote speech',
        'shorts education fact learn', 'shorts nature clip stunning', 'shorts car review quick',
        'shorts pet trick cute', 'shorts transformation glow up', 'shorts clean humor family',
        'shorts challenge attempt funny', 'shorts comparison versus quick', 'shorts history fact interesting',
        'shorts review product honest', 'shorts experiment result surprising', 'shorts craft DIY quick',
        'shorts garden tip useful', 'shorts home tip hack', 'shorts fitness form check',
        'shorts language learn phrase', 'shorts math trick mental', 'shorts coding tip quick',
        'shorts photography tip shot', 'shorts drawing tutorial quick', 'shorts piano song short',
        'shorts guitar riff cool', 'shorts singing impressive clip', 'shorts drone shot aerial',
        'shorts slowmo satisfying clip', 'shorts illusion mind trick', 'shorts talent impressive clip',
        'shorts wholesome moment nice', 'shorts fail funny compilation',
    ],
    'yt_watch': [
        'relaxing music 1 hour video', 'nature documentary full episode', 'cooking show full episode watch',
        'travel vlog new destination', 'tech review in depth analysis', 'educational lecture interesting topic',
        'podcast full episode listen', 'comedy special full show', 'movie analysis explained video',
        'book summary animation video', 'history documentary fascinating', 'science explained clearly video',
        'true crime documentary story', 'art tutorial step by step', 'gaming playthrough commentary',
        'home renovation transformation', 'car restoration project video', 'woodworking project build',
        'gardening full season video', 'camping adventure full trip', 'fishing trip full day video',
        'hiking trail full walkthrough', 'city tour walking video', 'museum tour virtual walk',
        'aquarium walk through video', 'zoo visit full tour', 'theme park ride through video',
        'concert full live performance', 'interview long form discussion', 'debate interesting topic watch',
        'TED talk inspiring speech', 'commencement speech motivational', 'lecture series university free',
        'tutorial comprehensive beginner', 'course lesson complete module', 'masterclass preview clip',
        'ASMR video long relaxing', 'ambience video background study', 'fireplace video crackling cozy',
        'rain sounds video relaxing sleep', 'ocean waves video calming', 'forest sounds birds video',
        'white noise machine video', 'study timer Pomodoro video', 'meditation guided video session',
        'yoga class full session', 'workout video full routine', 'stretching routine full body video',
        'dance tutorial full choreography', 'language lesson full class',
    ],
    'yt_gaming': [
        'gaming lets play new game', 'Minecraft build tutorial creative', 'Fortnite gameplay highlights',
        'GTA online funny moments', 'Roblox gameplay popular games', 'Valorant gameplay ranked',
        'League of Legends gameplay tips', 'Call of Duty gameplay new season', 'Elden Ring boss fight guide',
        'Zelda gameplay walkthrough', 'Mario game new release gameplay', 'Pokemon new game gameplay',
        'FIFA gameplay online match', 'Madden gameplay highlights', 'NBA 2K gameplay tips',
        'Hogwarts Legacy gameplay explore', 'Cyberpunk gameplay amazing moments', 'Starfield gameplay review',
        'indie game gameplay discover new', 'horror game gameplay scary', 'survival game gameplay tips',
        'racing game gameplay comparison', 'puzzle game gameplay solution', 'strategy game gameplay tutorial',
        'simulation game gameplay relaxing', 'fighting game gameplay combo', 'retro game gameplay nostalgia',
        'VR game gameplay experience', 'mobile game gameplay review', 'speedrun gaming world record',
        'gaming news discussion update', 'game review honest opinion', 'game comparison versus match',
        'gaming setup tour room', 'gaming PC build tutorial', 'console comparison performance test',
        'gaming monitor review comparison', 'gaming chair review comfort', 'gaming accessories review best',
        'Twitch stream highlights best', 'gaming rage quit funny moments', 'gaming glitch funny bugs',
        'esports tournament highlights', 'gaming tier list ranking', 'game theory analysis lore',
        'game soundtrack music listen', 'game trailer reaction new', 'game update patch notes review',
        'gaming challenge impossible attempt', 'gaming community drama news',
    ],
    'yt_cooking': [
        'easy recipe tutorial dinner tonight', 'baking tutorial from scratch', 'meal prep Sunday video',
        'street food tour video city', 'restaurant copycat recipe home', 'budget meal ideas cooking',
        'gourmet cooking at home tutorial', 'one pot recipe easy cleanup', 'air fryer recipe crispy',
        'slow cooker dump recipe easy', 'instant pot recipe quick meal', 'grilling recipe summer BBQ',
        'vegan recipe tasty tutorial', 'keto recipe low carb meal', 'gluten free recipe delicious',
        'dessert recipe from scratch baking', 'bread making tutorial artisan', 'pasta making fresh homemade',
        'sushi making at home tutorial', 'ramen recipe authentic bowl', 'curry recipe flavorful spice',
        'stir fry recipe wok technique', 'soup recipe comfort food', 'salad recipe filling meal',
        'smoothie recipe healthy breakfast', 'juice recipe fresh press', 'cocktail recipe bartending',
        'coffee brewing tutorial methods', 'cake decorating tutorial beautiful', 'cookie recipe crispy chewy',
        'pie recipe homemade flaky crust', 'pizza making dough to oven', 'taco recipe seasoning filling',
        'fried chicken recipe crispy juicy', 'steak cooking tutorial perfect sear', 'seafood recipe cooking fish',
        'breakfast recipe quick morning', 'brunch recipe weekend cooking', 'snack recipe healthy tasty',
        'appetizer recipe party hosting', 'sauce recipe homemade versatile', 'spice blend recipe custom mix',
        'food challenge eating video', 'mukbang eating show video', 'food review restaurant taste test',
        'cooking competition challenge video', 'chef tips professional technique', 'kitchen tour cookware review',
        'grocery haul cooking ingredients', 'pantry organization food storage', 'kitchen gadget testing review',
    ],
    'yt_news': [
        'news update today video', 'breaking news live coverage', 'world news analysis video',
        'political news debate discussion', 'economic news market analysis', 'tech news latest update',
        'science news discovery video', 'health news research update', 'climate news environment video',
        'sports news highlights recap', 'entertainment news celebrity update', 'business news company update',
        'local news community report', 'investigative journalism expose', 'opinion editorial news video',
        'fact check debunk viral claim', 'explained video complex topic', 'deep dive journalism story',
        'news documentary short form', 'news interview political leader', 'press conference live video',
        'news roundup weekly summary', 'morning news show highlights', 'evening news broadcast clip',
        'news channel comparison analysis', 'independent journalism video', 'citizen journalism viral video',
        'news podcast video episode', 'current events discussion panel', 'debate show political topics',
        'financial news stock market', 'cryptocurrency news market update', 'real estate news market',
        'education news school policy', 'healthcare news hospital system', 'transportation news infrastructure',
        'energy news renewable fossil', 'agriculture news farming food', 'technology regulation news policy',
        'social media news platform changes', 'privacy news data security', 'AI news regulation debate',
        'space news mission launch update', 'military defense news update', 'disaster relief news coverage',
        'human interest news story feel good', 'protest movement news coverage', 'election news coverage analysis',
        'UN international organization news', 'trade agreement news policy', 'immigration news policy debate',
    ],
    'yt_education': [
        'educational video interesting topic', 'how things work explained', 'science explained animation',
        'history lesson engaging video', 'math tutorial explained clearly', 'language learning lesson video',
        'geography interesting facts video', 'philosophy explained simply video', 'psychology concept explained',
        'economics basics explained video', 'physics concepts animation', 'chemistry experiment video safe',
        'biology nature documentary', 'engineering explained how built', 'architecture explained design',
        'astronomy space facts video', 'geology earth science video', 'ecology environment explained',
        'sociology concepts explained video', 'political science explained', 'law legal concepts explained',
        'medicine health explained video', 'nutrition food science video', 'technology explained simply',
        'programming coding tutorial free', 'web development tutorial series', 'data science tutorial video',
        'graphic design tutorial free', 'music theory lesson video', 'art history lesson video',
        'literature analysis discussion', 'writing tips creative nonfiction', 'public speaking tips video',
        'debate skills improvement video', 'critical thinking exercises video', 'logic puzzles brain teasers',
        'study techniques effective video', 'memory improvement tips video', 'speed reading technique video',
        'note taking method tutorial', 'exam preparation strategy video', 'college admission advice video',
        'scholarship application tips video', 'career guidance counseling video', 'skill development free course',
        'certification exam preparation', 'workshop lecture free online', 'seminar conference presentation',
        'TED talk educational inspiring', 'academic lecture recorded free', 'research methodology tutorial',
    ],
    'yt_tech_review': [
        'smartphone review latest model', 'laptop review performance test', 'tablet review comparison',
        'smartwatch review features', 'earbuds review sound quality', 'headphones review comparison',
        'camera review photography video', 'drone review features flight', 'gaming console review test',
        'monitor review for work gaming', 'keyboard review mechanical wireless', 'mouse review ergonomic gaming',
        'webcam review video call', 'microphone review podcasting', 'speaker review bluetooth smart',
        'TV review OLED QLED compare', 'projector review home theater', 'streaming device review',
        'router review wifi speed', 'smart home hub review', 'smart doorbell review security',
        'robot vacuum review cleaning', 'air purifier review performance', 'electric toothbrush review',
        'fitness tracker review accuracy', 'VR headset review experience', 'e-reader review comparison',
        'power bank review capacity', 'USB hub review connectivity', 'SSD review speed benchmark',
        'graphics card review gaming', 'CPU review performance benchmark', 'RAM review speed test',
        'PC build guide budget 2025', 'Mac vs PC comparison review', 'Chromebook review worth it',
        'software review productivity tool', 'app review useful new', 'cloud service review comparison',
        'VPN review privacy speed', 'password manager review security', 'antivirus review protection',
        'photo editing software review', 'video editing software review', 'music production DAW review',
        'coding IDE review comparison', 'AI tool review productivity', 'electric bike review range',
        'electric scooter review commute', 'dash cam review safety', 'car tech review features',
    ],
    'yt_vlogs': [
        'daily vlog life routine', 'travel vlog new destination explore', 'moving vlog new city apartment',
        'college vlog student life', 'work vlog day in office', 'shopping vlog haul store',
        'cooking vlog recipe making', 'cleaning vlog home organization', 'workout vlog gym routine',
        'weekend vlog activities fun', 'holiday vlog celebration family', 'birthday vlog celebration party',
        'wedding vlog beautiful ceremony', 'pregnancy journey vlog updates', 'new baby vlog parenting',
        'pet vlog day with animal', 'car vlog drive scenic route', 'bike vlog cycling adventure',
        'hiking vlog trail nature', 'beach vlog relaxing day', 'camping vlog outdoor adventure',
        'city walk vlog exploring streets', 'museum vlog visit art culture', 'restaurant vlog food review',
        'cafe vlog coffee studying', 'night out vlog friends fun', 'date vlog romantic evening',
        'solo day vlog self care', 'rainy day vlog cozy indoor', 'productive day vlog routine',
        'minimalist life vlog lifestyle', 'luxury life vlog experience', 'budget life vlog saving tips',
        'expat vlog living abroad', 'digital nomad vlog working traveling', 'van life vlog adventure road',
        'tiny house vlog living small', 'apartment tour vlog decorating', 'house hunting vlog search',
        'renovation vlog home project', 'garden vlog growing plants', 'thrift vlog shopping finds',
        'unboxing vlog packages delivery', 'organizing vlog declutter home', 'studying vlog exam prep',
        'creative vlog art making process', 'music vlog practicing performing', 'dance vlog rehearsal performance',
        'fashion vlog outfit ideas week', 'beauty vlog routine products', 'wellness vlog health journey',
    ],
    'yt_sports': [
        'sports highlights best plays today', 'football match highlights goals', 'basketball game highlights dunks',
        'baseball game highlights home runs', 'hockey game highlights goals saves', 'soccer goals best compilation',
        'tennis match highlights rally', 'golf tournament highlights shots', 'boxing fight highlights rounds',
        'MMA UFC fight highlights finish', 'wrestling match highlights moves', 'F1 race highlights overtakes',
        'NASCAR race highlights finish', 'cycling race highlights sprint', 'swimming race highlights record',
        'track field highlights race', 'gymnastics routine highlights score', 'figure skating highlights jumps',
        'skiing snowboard highlights tricks', 'surfing competition highlights waves', 'skateboarding highlights tricks',
        'extreme sports compilation best', 'sports fails funny compilation', 'sports emotional moments touching',
        'sports comeback incredible story', 'sports record breaking moment', 'sports draft analysis picks',
        'sports trade analysis reaction', 'sports predictions season outlook', 'fantasy sports advice tips',
        'sports workout training athlete', 'sports nutrition diet plan', 'sports injury prevention tips',
        'sports equipment review gear', 'sports shoe review performance', 'sports apparel review comfort',
        'sports coaching tips technique', 'sports strategy analysis play', 'sports history greatest moments',
        'sports documentary athlete story', 'sports interview player coach', 'sports debate hot take show',
        'sports podcast episode discussion', 'sports betting analysis odds', 'sports trivia quiz fun',
        'youth sports development tips', 'women sports highlights coverage', 'paralympic sports highlights inspiring',
        'college sports highlights game', 'amateur sports competition video', 'sports fan reaction video',
    ],
    'yt_fitness': [
        'home workout full body beginner', 'HIIT workout 20 minutes intense', 'yoga flow morning stretch',
        'pilates core workout beginner', 'strength training dumbbells workout', 'cardio workout no equipment',
        'abs workout routine six pack', 'arm workout toning exercise', 'leg workout squat routine',
        'glute workout hip exercises', 'back workout posture correction', 'chest workout push up routine',
        'shoulder workout dumbbell routine', 'flexibility routine full body stretch', 'mobility workout joint health',
        'balance training exercises stability', 'resistance band workout full body', 'kettlebell workout routine',
        'boxing workout cardio fitness', 'dance workout fun cardio', 'barre workout toning class',
        'swimming workout pool exercise', 'cycling workout indoor trainer', 'running form technique video',
        'walking workout power walk', 'stair workout climbing exercise', 'jump rope workout routine',
        'calisthenics workout bodyweight', 'CrossFit workout WOD video', 'functional training exercises daily',
        'senior fitness exercise routine', 'prenatal workout safe exercise', 'postpartum workout recovery',
        'physical therapy exercises rehab', 'foam rolling recovery routine', 'cool down stretch routine',
        'warmup routine before exercise', 'sports specific training workout', 'marathon training long run video',
        'muscle building workout plan', 'fat burning workout effective', 'endurance training workout video',
        'athletic performance training tips', 'fitness motivation workout montage', 'fitness transformation story',
        'workout challenge 30 day program', 'gym tour fitness equipment', 'home gym setup budget guide',
        'fitness tracker workout data review', 'fitness app workout program review', 'personal trainer session video',
    ],
    'yt_comedy': [
        'funny video compilation laugh hard', 'stand up comedy clip funny', 'comedy sketch hilarious short',
        'prank video harmless funny reaction', 'fail compilation funny moments', 'funny animal video cats dogs',
        'funny baby video cute moments', 'comedy podcast clip funny discussion', 'improv comedy show clip',
        'roast comedy clip funny burns', 'funny commercial compilation ads', 'blooper reel behind scenes funny',
        'parody video funny impression', 'meme review funny commentary', 'funny voiceover video narration',
        'awkward moments compilation cringe funny', 'funny interview moments celebrity', 'game show funny moments',
        'news blooper funny mistake', 'sports funny moments epic fails', 'wedding funny moments compilation',
        'public speaking funny moments', 'office humor workplace funny video', 'school funny moments students',
        'cooking fail funny kitchen disaster', 'DIY fail funny attempt', 'auto tune funny compilation',
        'lip sync battle funny performance', 'funny dance video compilation', 'comedy duo funny skit',
        'satirical news comedy show clip', 'comedy movie clip best scene', 'sitcom funny moments best',
        'animation comedy funny cartoon', 'dark humor comedy special', 'observational comedy relatable',
        'physical comedy slapstick funny', 'musical comedy funny song', 'character comedy impersonation',
        'comedy magic show funny tricks', 'ventriloquist comedy show funny', 'comedy variety show best moments',
        'dad jokes funny reaction video', 'pun comedy wordplay humor', 'clean comedy family friendly',
        'international comedy different cultures', 'classic comedy vintage clips', 'comedy reaction try not laugh',
        'hidden camera comedy show prank', 'street comedy busker funny', 'comedy competition show clip',
    ],
    'yt_documentary': [
        'documentary nature wildlife full', 'documentary history fascinating episode', 'documentary science breakthrough full',
        'documentary true crime investigation', 'documentary space universe exploration', 'documentary ocean deep sea life',
        'documentary music artist biography', 'documentary sports athlete story', 'documentary food culture travel',
        'documentary technology innovation future', 'documentary art creative process', 'documentary society culture issue',
        'documentary war conflict survivor', 'documentary environment climate change', 'documentary economics finance system',
        'documentary health medical science', 'documentary psychology human mind', 'documentary education system world',
        'documentary politics government power', 'documentary religion faith belief', 'documentary philosophy ideas thinkers',
        'documentary architecture design buildings', 'documentary fashion industry inside', 'documentary automotive cars racing',
        'documentary aviation flight history', 'documentary railway train travel', 'documentary ships maritime history',
        'documentary exploration adventure expedition', 'documentary mountain climbing summit', 'documentary cave exploration deep',
        'documentary archaeology ancient discovery', 'documentary anthropology cultures tribes', 'documentary photography visual story',
        'documentary film making behind scenes', 'documentary journalism investigation report', 'documentary espionage spy story',
        'documentary prison system inside', 'documentary cult organization story', 'documentary disaster survival story',
        'documentary rescue mission heroic', 'documentary engineering marvel construction', 'documentary city urban development',
        'documentary rural life countryside', 'documentary island life remote', 'documentary arctic ice expedition',
        'documentary desert survival journey', 'documentary rainforest biodiversity', 'documentary volcano eruption study',
        'documentary earthquake aftermath recovery', 'documentary pandemic response global', 'documentary innovation startup story',
    ],
    'yt_asmr': [
        'ASMR whisper trigger relaxing', 'ASMR tapping sounds sleep', 'ASMR eating food sounds',
        'ASMR roleplay scenario calming', 'ASMR keyboard typing sounds', 'ASMR rain sounds ambient',
        'ASMR hair brushing sounds gentle', 'ASMR page turning book sounds', 'ASMR crinkle sounds paper',
        'ASMR water sounds flowing', 'ASMR nature sounds forest birds', 'ASMR fire crackling sounds',
        'ASMR cooking sounds kitchen', 'ASMR drawing sketching sounds', 'ASMR painting art sounds',
        'ASMR cleaning satisfying sounds', 'ASMR organizing sorting sounds', 'ASMR craft making sounds',
        'ASMR soap cutting satisfying', 'ASMR slime sounds satisfying', 'ASMR sand kinetic satisfying',
        'ASMR massage sounds relaxing', 'ASMR spa treatment sounds', 'ASMR scalp massage sounds',
        'ASMR face touching sounds gentle', 'ASMR personal attention roleplay', 'ASMR sleep aid sounds long',
        'ASMR study ambient sounds focus', 'ASMR reading aloud calm voice', 'ASMR storytelling bedtime',
        'ASMR trigger assortment variety', 'ASMR no talking sounds only', 'ASMR soft spoken calming',
        'ASMR mouth sounds subtle', 'ASMR breathing sounds relaxation', 'ASMR ear to ear audio',
        'ASMR 3D binaural audio immersive', 'ASMR medical exam roleplay', 'ASMR barber shop roleplay',
        'ASMR library ambience study', 'ASMR coffee shop ambience work', 'ASMR bookstore ambience cozy',
        'ASMR cozy room ambience relax', 'ASMR winter ambience snowfall', 'ASMR spring ambience garden',
        'ASMR ocean waves beach sleep', 'ASMR thunderstorm heavy rain', 'ASMR wind howling ambient',
        'ASMR crystal sounds tinkling', 'ASMR wood sounds carving',
    ],
    'yt_travel': [
        'travel vlog destination adventure new', 'city tour walking exploration video', 'food tour local cuisine video',
        'hotel resort review stay experience', 'airline flight review experience video', 'train journey scenic video',
        'road trip adventure driving video', 'cruise ship tour experience video', 'backpacking trip budget adventure',
        'luxury travel experience video', 'solo travel adventure story', 'couple travel vlog destination',
        'family travel with kids video', 'adventure travel extreme activity', 'cultural travel immersion video',
        'beach destination tropical paradise video', 'mountain destination hiking trekking', 'island hopping adventure video',
        'desert safari adventure video', 'jungle rainforest exploration video', 'arctic northern lights video',
        'European city tour guide video', 'Asian destination travel guide', 'African safari adventure video',
        'South American travel adventure', 'Australian outback travel video', 'Caribbean island travel guide',
        'Middle East travel destination video', 'Southeast Asian backpacking video', 'Central American travel adventure',
        'hidden gem destination discover', 'off beaten path travel explore', 'digital nomad destination review',
        'expat life abroad experience video', 'travel tips tricks hacks', 'packing tips efficient travel',
        'travel gear review essentials', 'travel photography tips video', 'travel budget planning tips',
        'travel safety tips abroad video', 'travel documentary series watch', 'virtual travel experience video',
        'UNESCO world heritage site visit', 'national park visit hike video', 'theme park review ride video',
        'museum gallery tour visit video', 'market bazaar tour shopping video', 'festival celebration travel video',
        'travel Q&A tips advice', 'travel transformation journey story', 'travel comparison destinations',
    ],
    'yt_diy': [
        'DIY project home improvement video', 'woodworking project build tutorial', 'furniture making DIY video',
        'home decor DIY project easy', 'craft project handmade tutorial', 'painting technique wall DIY',
        'tiling installation tutorial DIY', 'plumbing fix repair tutorial', 'electrical fix wiring DIY safe',
        'car repair maintenance DIY video', 'bike repair maintenance tutorial', 'sewing project beginner tutorial',
        'knitting crochet project tutorial', 'jewelry making DIY tutorial', 'candle making tutorial process',
        'soap making tutorial handmade', 'pottery clay project tutorial', 'resin art project tutorial',
        'leather crafting project DIY', 'metalworking welding project tutorial', 'glass art stained glass DIY',
        'paper craft origami tutorial', 'scrapbooking creative project', 'calligraphy lettering tutorial',
        'screen printing T-shirt DIY', 'tie dye project colorful tutorial', 'embroidery cross stitch tutorial',
        'macrame wall hanging tutorial', 'terrarium building tutorial plant', 'aquarium setup DIY project',
        'outdoor furniture build project', 'garden bed raised build DIY', 'shed building project tutorial',
        'fence building repair tutorial', 'deck building staining project', 'concrete project patio walkway',
        'fire pit build outdoor project', 'water feature fountain DIY', 'chicken coop build tutorial',
        'birdhouse feeder build project', 'tool organization workshop setup', 'workbench build tutorial garage',
        'storage solution build organize', 'kids playhouse build tutorial', 'treehouse platform build',
        'desk build custom workspace', 'shelf build floating mounted', 'headboard build bedroom project',
        'kitchen island build DIY project', 'bathroom vanity build custom',
    ],

    # ──────────────────────────────────────────────────────────────────
    # MAPS variants (50 queries each)
    # ──────────────────────────────────────────────────────────────────
    'maps_restaurants': [
        'restaurants near me', 'best rated restaurants nearby', 'Italian restaurant near me',
        'Chinese restaurant near me', 'Mexican food near me', 'Indian restaurant near me',
        'Thai restaurant near me', 'Japanese restaurant near me', 'Korean restaurant near me',
        'Vietnamese restaurant near me', 'French restaurant near me', 'Greek restaurant near me',
        'Mediterranean food near me', 'pizza places near me', 'burger places near me',
        'sushi near me', 'ramen near me', 'tacos near me',
        'seafood restaurant near me', 'steakhouse near me', 'BBQ near me',
        'vegan restaurants near me', 'vegetarian restaurants near me', 'gluten free restaurants',
        'brunch places near me', 'breakfast restaurants open now', 'lunch specials near me',
        'dinner restaurants romantic', 'fine dining near me', 'cheap eats near me',
        'food trucks near me', 'buffet near me', 'all you can eat near me',
        'fast food near me', 'drive through open near me', 'delivery restaurants near me',
        'outdoor dining restaurants near me', 'restaurants with live music', 'family restaurants near me',
        'restaurants open late near me', 'restaurants open now near me', 'new restaurants recently opened',
        'restaurants with best reviews', 'halal food near me', 'kosher restaurant near me',
        'farm to table restaurants near me', 'brunch with mimosas near me', 'sports bars near me',
        'rooftop restaurants near me', 'waterfront dining near me',
    ],
    'maps_directions': [
        'directions to nearest grocery store', 'directions to airport', 'directions to hospital',
        'directions to nearest gas station', 'driving directions downtown', 'walking directions to park',
        'transit directions to work', 'bike route to school', 'directions to train station',
        'directions to bus stop near me', 'fastest route to highway', 'directions avoiding tolls',
        'directions to nearest pharmacy', 'route to shopping mall', 'directions to post office',
        'directions to library near me', 'directions to city center', 'directions to beach',
        'directions to mountains hiking', 'scenic route directions drive', 'directions to nearest hotel',
        'directions to car dealership', 'route to mechanic near me', 'directions to vet clinic',
        'directions to dentist near me', 'directions to bank near me', 'directions to police station',
        'directions to fire station', 'directions to school near me', 'directions to university campus',
        'directions to stadium arena', 'directions to concert venue', 'directions to convention center',
        'directions to nearest ATM', 'directions to car wash', 'directions to dry cleaner',
        'directions to hardware store', 'directions to pet store', 'directions to movie theater',
        'directions to bowling alley', 'directions to amusement park', 'directions to zoo',
        'directions to aquarium', 'directions to botanical garden', 'directions to nearest campground',
        'directions to boat launch', 'directions to ski resort', 'directions to outlet mall',
        'directions to farmers market', 'directions to flea market',
    ],
    'maps_places': [
        'things to do near me', 'tourist attractions near me', 'points of interest nearby',
        'places to visit this weekend', 'popular places near me', 'landmarks near me',
        'scenic viewpoints near me', 'historic sites near me', 'nature spots near me',
        'photography spots near me', 'Instagram worthy spots nearby', 'hidden gems near me',
        'free activities near me', 'kid friendly places near me', 'dog friendly places near me',
        'romantic spots near me', 'quiet places near me', 'outdoor activities near me',
        'indoor activities near me', 'rainy day activities near me', 'adventure activities near me',
        'art galleries near me', 'theaters near me', 'live music venues near me',
        'comedy clubs near me', 'escape rooms near me', 'mini golf near me',
        'trampoline park near me', 'laser tag near me', 'arcade near me',
        'rock climbing gym near me', 'ice skating rink near me', 'swimming pool near me',
        'batting cages near me', 'go kart racing near me', 'paintball near me',
        'axe throwing near me', 'pottery studio near me', 'cooking classes near me',
        'wine tasting near me', 'brewery tour near me', 'distillery near me',
        'community garden near me', 'farmers market near me', 'flea market near me',
        'antique shops near me', 'bookstores near me', 'record stores near me',
        'thrift stores near me', 'outlet stores near me',
    ],
    'maps_photos': [
        'scenic spots photos near me', 'sunset viewpoint photos', 'nature photography locations',
        'city skyline photo spots', 'waterfall photos location', 'beach photos beautiful location',
        'mountain view photo spots', 'lake photos scenic spot', 'forest hiking trail photos',
        'garden photos botanical nearby', 'bridge photos iconic location', 'architecture photos buildings',
        'street art murals photo locations', 'park photos beautiful nearby', 'river walk photos scenic',
        'lighthouse photos location visit', 'castle ruins photos historic', 'church cathedral photos beautiful',
        'colorful buildings photos location', 'rooftop views photos city', 'harbor marina photos boats',
        'vineyard winery photos visit', 'farm field photos rural', 'desert landscape photos location',
        'canyon photos dramatic views', 'cave photos accessible near', 'island photos paradise visit',
        'dock pier photos waterfront', 'boardwalk photos coastal', 'plaza square photos charming',
        'market photos vibrant colorful', 'festival event photos local', 'snow covered landscape photos',
        'autumn foliage photos location', 'cherry blossom photos location', 'wildflower field photos spring',
        'sunrise photos location early', 'blue hour photos city lights', 'starry sky photos dark location',
        'reflection photos water mirror', 'foggy landscape photos moody', 'aerial view photos location',
        'panoramic view photos wide', 'close up nature photos macro', 'animal wildlife photos location',
        'bird watching photos location', 'butterfly garden photos visit', 'zen garden photos peaceful',
        'sculpture garden photos art', 'mural wall photos colorful',
    ],
    'maps_reviews': [
        'best reviewed places near me', 'top rated restaurants reviews', 'hotel reviews near me best',
        'dentist reviews near me top', 'doctor reviews near me best', 'mechanic reviews trustworthy',
        'salon reviews near me best', 'gym reviews near me top', 'spa reviews near me relaxing',
        'vet reviews near me trusted', 'school reviews near me rated', 'daycare reviews near me',
        'contractor reviews near me reliable', 'plumber reviews near me honest', 'electrician reviews near me',
        'lawyer reviews near me recommended', 'accountant reviews near me trusted', 'bank reviews near me',
        'insurance agent reviews near me', 'real estate agent reviews local', 'moving company reviews near me',
        'storage facility reviews near me', 'car wash reviews near me', 'dry cleaner reviews near me',
        'tailor alterations reviews near me', 'shoe repair reviews near me', 'pet groomer reviews near me',
        'dog walker reviews near me', 'house cleaner reviews near me', 'landscaper reviews near me',
        'pool service reviews near me', 'pest control reviews near me', 'HVAC reviews near me',
        'roofer reviews near me', 'painter reviews near me', 'locksmith reviews near me',
        'towing service reviews near me', 'auto body shop reviews near me', 'tire shop reviews near me',
        'eye doctor reviews near me', 'chiropractor reviews near me', 'physical therapist reviews near me',
        'dermatologist reviews near me', 'pediatrician reviews near me', 'orthodontist reviews near me',
        'yoga studio reviews near me', 'dance studio reviews near me', 'music school reviews near me',
        'tutoring center reviews near me', 'printing shop reviews near me',
    ],
    'maps_street': [
        'street view famous landmarks', 'street view city center tour', 'street view beach coastline',
        'street view mountain road scenic', 'street view historic district', 'street view downtown area',
        'street view shopping street', 'street view park entrance', 'street view university campus',
        'street view stadium exterior', 'street view bridge famous', 'street view harbor waterfront',
        'street view market area bustling', 'street view residential area nice', 'street view suburban neighborhood',
        'street view rural countryside', 'street view vineyard winery', 'street view castle exterior',
        'street view cathedral church', 'street view museum exterior', 'street view train station',
        'street view airport terminal', 'street view hotel entrance', 'street view restaurant row',
        'street view cafe district', 'street view art district', 'street view entertainment area',
        'street view financial district', 'street view government buildings', 'street view embassy row',
        'street view botanical garden entrance', 'street view zoo entrance', 'street view theme park',
        'street view sports complex', 'street view hospital medical center', 'street view school building',
        'street view library public', 'street view community center', 'street view fire station',
        'street view police headquarters', 'street view post office', 'street view courthouse',
        'street view city hall', 'street view monument memorial', 'street view plaza square',
        'street view fountain famous', 'street view statue sculpture', 'street view tower observation',
        'street view pier dock walk', 'street view boardwalk promenade',
    ],
    'maps_gas_stations': [
        'gas stations near me', 'cheapest gas near me', 'gas station open 24 hours',
        'Shell gas station near me', 'BP gas station near me', 'ExxonMobil near me',
        'Chevron gas station near me', 'Costco gas station near me', 'Sam\'s Club gas near me',
        'diesel fuel near me', 'premium gas station near me', 'gas station with car wash',
        'gas station with air pump', 'gas station with EV charging', 'electric vehicle charging station',
        'Tesla supercharger near me', 'EV fast charging station', 'gas station with food mart',
        'gas station with restroom clean', 'truck stop near me', 'gas station highway exit',
        'gas station prices comparison nearby', 'gas station open now near me',
    ],
    'maps_hotels': [
        'hotels near me tonight', 'cheap hotels near me', 'luxury hotels near me',
        'best rated hotels nearby', 'hotels with pool near me', 'pet friendly hotels near me',
        'hotels with free breakfast', 'boutique hotels near me', 'hotels near airport',
        'hotels downtown city center', 'extended stay hotels near me', 'all inclusive resorts',
        'bed and breakfast near me', 'hostels near me cheap', 'vacation rentals near me',
        'Airbnb near me available', 'motels near me cheap', 'hotels with gym near me',
        'hotels with spa near me', 'romantic hotels near me', 'family friendly hotels near me',
        'business hotels near me', 'conference hotels near me', 'hotels near hospital',
        'hotels with parking free', 'hotels with kitchen suite', 'wheelchair accessible hotels',
    ],
    'maps_hospitals': [
        'hospital near me emergency', 'urgent care near me open now', 'emergency room near me',
        'hospital near me 24 hours', 'children\'s hospital near me', 'medical center near me',
        'clinic walk in near me', 'specialist hospital near me', 'mental health facility near me',
        'rehabilitation center near me', 'cancer treatment center near me', 'heart hospital near me',
        'orthopedic hospital near me', 'eye hospital near me', 'maternity hospital near me',
        'dental emergency near me', 'psychiatric hospital near me', 'veteran hospital near me',
        'community health center near me', 'free clinic near me', 'lab testing center near me',
    ],
    'maps_parks': [
        'parks near me open', 'national parks near me', 'state parks near me',
        'dog parks near me', 'playgrounds near me', 'botanical gardens near me',
        'nature trails near me', 'hiking parks near me', 'picnic areas near me',
        'waterfront parks near me', 'parks with basketball courts', 'parks with tennis courts',
        'parks with swimming pools', 'parks with splash pads', 'parks with skateparks',
        'parks with camping', 'parks with fishing', 'urban parks near me',
        'wildlife refuges near me', 'bird watching parks near me', 'gardens near me beautiful',
        'arboretum near me', 'conservation area near me', 'nature reserve near me',
    ],
    'maps_shopping_malls': [
        'shopping malls near me', 'outlet malls near me', 'shopping centers near me',
        'best mall near me', 'mall with movie theater', 'mall with food court',
        'largest mall near me', 'indoor mall near me', 'outdoor shopping center',
        'premium outlet stores near me', 'mall stores directory near me', 'mall hours today',
        'shopping district near me', 'department stores near me', 'clothing stores near me',
        'electronics stores near me', 'shoe stores near me', 'jewelry stores near me',
        'furniture stores near me', 'home goods stores near me', 'sporting goods stores near me',
    ],
    'maps_coffee_shops': [
        'coffee shops near me', 'Starbucks near me', 'best coffee near me',
        'independent coffee shop near me', 'coffee shop with wifi', 'quiet coffee shop study',
        'coffee shop open now', 'coffee shop with outdoor seating', 'coffee roaster near me',
        'espresso bar near me', 'cafe with food near me', 'brunch cafe near me',
        'tea house near me', 'boba tea near me', 'matcha cafe near me',
        'pastry cafe near me', 'coffee shop with books', 'coffee shop cozy atmosphere',
        'specialty coffee shop near me', 'coffee drive through near me', 'late night coffee shop open',
    ],
    'maps_gyms': [
        'gyms near me', 'cheap gym near me', 'gym with pool near me',
        'Planet Fitness near me', '24 hour fitness gym near me', 'CrossFit gym near me',
        'yoga studio near me', 'Pilates studio near me', 'martial arts gym near me',
        'boxing gym near me', 'rock climbing gym near me', 'spin class studio near me',
        'gym with sauna near me', 'gym with classes near me', 'personal trainer gym near me',
        'women only gym near me', 'gym with basketball court', 'gym with track running',
        'outdoor fitness park near me', 'gym free trial near me', 'gym day pass near me',
    ],
    'maps_pharmacies': [
        'pharmacy near me open now', '24 hour pharmacy near me', 'CVS pharmacy near me',
        'Walgreens near me', 'Rite Aid near me', 'pharmacy with drive through',
        'pharmacy prescription ready', 'cheapest pharmacy near me', 'pharmacy with clinic',
        'compounding pharmacy near me', 'pharmacy that delivers near me', 'pharmacy open Sunday',
        'specialty pharmacy near me', 'pharmacy with immunizations', 'discount pharmacy near me',
    ],
    'maps_banks': [
        'banks near me', 'ATM near me free', 'Chase bank near me',
        'Bank of America near me', 'Wells Fargo near me', 'credit union near me',
        'bank open Saturday near me', 'bank with notary near me', 'bank drive through near me',
        'bank with safe deposit boxes', 'bank branch near me hours', 'coin counting machine near me',
        'money order near me', 'wire transfer bank near me', 'currency exchange near me',
    ],
    'maps_supermarkets': [
        'supermarket near me', 'grocery store near me open', 'Walmart near me',
        'Target near me', 'Whole Foods near me', 'Trader Joe\'s near me',
        'Costco near me', 'Aldi near me', 'Kroger near me',
        'organic grocery store near me', 'Asian grocery store near me', 'Mexican grocery store near me',
        'Indian grocery store near me', 'halal grocery store near me', 'international food store near me',
        'discount grocery store near me', 'grocery store open 24 hours', 'grocery delivery near me',
        'farmers market near me today', 'butcher shop near me', 'fish market near me',
    ],
    'maps_museums': [
        'museums near me', 'art museum near me', 'history museum near me',
        'science museum near me', 'children\'s museum near me', 'natural history museum near me',
        'war museum near me', 'air and space museum near me', 'maritime museum near me',
        'automotive museum near me', 'photography museum near me', 'design museum near me',
        'free museums near me', 'museum open today near me', 'interactive museum near me',
        'museum exhibits current near me', 'museum with guided tours', 'outdoor museum near me',
    ],

    # ──────────────────────────────────────────────────────────────────
    # NEWS variants (30 queries each)
    # ──────────────────────────────────────────────────────────────────
    'news_headlines': [
        'top news stories today', 'breaking news live updates', 'headline news this morning',
        'today\'s top headlines summary', 'latest news update now', 'most important news today',
        'trending news stories now', 'front page news today', 'major news events today',
        'news highlights today summary', 'world headlines today top', 'national news today important',
        'news briefing today morning', 'evening news headlines today', 'overnight news developments',
        'weekend news summary highlights', 'news you missed today', 'top stories this week recap',
        'biggest news stories month', 'developing stories today follow', 'news alerts today urgent',
        'editorial news opinion today', 'news analysis in depth today', 'investigative news reports',
        'human interest news stories', 'feel good news today positive', 'news roundup daily digest',
        'news from around the world', 'news by topic browse', 'latest news all categories',
    ],
    'news_tech': [
        'tech news latest today', 'AI technology news breakthrough', 'smartphone release news',
        'social media platform update news', 'cybersecurity breach news', 'startup funding news round',
        'tech company earnings news report', 'cloud computing news developments', 'chip semiconductor news',
        'electric vehicle technology news', 'software update release news', 'app store policy news change',
        'tech regulation government news', 'internet privacy news policy', 'data breach notification news',
        'gaming technology news release', 'VR AR technology news update', 'blockchain technology news',
        'quantum computing news research', 'space technology news launch', 'robotics automation news',
        'biotech research news discovery', 'cleantech renewable energy news', 'fintech banking news',
        'edtech education technology news', 'health tech wearable news', 'smart home IoT news',
        'tech layoffs hiring news', 'tech conference event news', 'tech review product launch news',
    ],
    'news_sports': [
        'sports news today scores', 'football soccer news transfer', 'basketball NBA news trade',
        'baseball MLB news standings', 'hockey NHL news scores', 'tennis news tournament results',
        'golf news tournament standings', 'boxing MMA fight news', 'cricket news match results',
        'rugby news tournament', 'Formula 1 news race results', 'Olympics news athlete',
        'college sports news game', 'esports news tournament', 'women sports news growth',
        'sports injury news update', 'sports contract news signing', 'sports coaching news hiring',
        'sports venue news stadium', 'sports draft news picks', 'sports trade deadline news',
        'sports free agency news', 'sports suspension ban news', 'sports record breaking news',
        'sports controversy news debate', 'sports charity news event', 'sports comeback news story',
        'sports retirement announcement news', 'sports hall of fame news', 'sports preview season outlook',
    ],
    'news_entertainment': [
        'entertainment news today celebrity', 'movie release news premiere', 'TV show news season renewal',
        'music news album release', 'celebrity news gossip today', 'awards ceremony news winners',
        'streaming service news content', 'Broadway theater news shows', 'book publishing news release',
        'comedy show news special', 'festival concert news lineup', 'reality TV show news cast',
        'animation movie news studio', 'documentary release news premiere', 'podcast news popular new',
        'influencer news social media', 'fashion show news runway', 'art exhibition news gallery',
        'video game release news launch', 'comic book news release', 'fan convention news event',
        'entertainment industry news business', 'box office news numbers', 'ratings news TV show',
        'casting news movie role', 'director producer news project', 'entertainment controversy news debate',
        'nostalgia entertainment news revival', 'tribute memorial entertainment news', 'entertainment preview upcoming',
    ],
    'news_business': [
        'business news today markets', 'stock market news movement', 'economic news indicators',
        'company earnings news report', 'merger acquisition news deal', 'startup news funding round',
        'real estate business news', 'retail business news store', 'technology business news growth',
        'banking finance news regulation', 'cryptocurrency business news', 'oil energy business news',
        'automotive industry news sales', 'airline travel industry news', 'healthcare industry news',
        'manufacturing industry news', 'agriculture business news crop', 'supply chain news logistics',
        'trade tariff news policy', 'labor market news employment', 'small business news support',
        'franchise business news growth', 'IPO news company listing', 'bankruptcy news company filing',
        'CEO executive news appointment', 'business strategy news pivot', 'innovation business news patent',
        'ESG sustainability business news', 'consumer spending news trend', 'business forecast economic outlook',
    ],
    'news_health': [
        'health news today research', 'medical breakthrough news discovery', 'vaccine news development',
        'mental health news awareness', 'nutrition study news finding', 'fitness health news trend',
        'disease outbreak news alert', 'drug approval news FDA', 'hospital healthcare news system',
        'public health news policy', 'aging research news longevity', 'cancer research news treatment',
        'heart disease news prevention', 'diabetes news management', 'obesity health news study',
        'sleep research news finding', 'stress anxiety news management', 'immune system news research',
        'gut health news microbiome', 'genetic research news discovery', 'telemedicine health news growth',
        'health insurance news policy', 'prescription drug news pricing', 'alternative medicine news study',
        'pediatric health news children', 'women health news research', 'men health news study',
        'elderly health news care', 'global health news WHO', 'health technology news wearable',
    ],
    'news_science': [
        'science news today discovery', 'space science news exploration', 'climate science news research',
        'biology science news finding', 'physics science news breakthrough', 'chemistry science news discovery',
        'geology science news earthquake', 'ocean science news marine', 'ecology science news environment',
        'paleontology science news fossil', 'archaeology science news dig', 'anthropology science news',
        'neuroscience news brain research', 'genetics science news DNA', 'astronomy science news star',
        'quantum science news experiment', 'materials science news develop', 'energy science news fusion',
        'AI science news research paper', 'robotics science news advance', 'nanotechnology science news',
        'biotechnology science news modify', 'agricultural science news crop', 'food science news nutrition',
        'environmental science news pollution', 'conservation science news species', 'weather science news predict',
        'volcano science news eruption', 'earthquake science news seismic', 'science funding news grant',
    ],
    'news_world': [
        'world news today headline', 'international relations news', 'United Nations news resolution',
        'European Union news policy', 'Asia news developments', 'Africa news progress',
        'Middle East news situation', 'Latin America news event', 'diplomacy news agreement',
        'conflict news ceasefire', 'humanitarian news crisis aid', 'refugee news migration',
        'trade news agreement tariff', 'sanctions news country', 'election news country vote',
        'protest news demonstration', 'natural disaster news response', 'pandemic news global update',
        'climate summit news agreement', 'nuclear policy news treaty', 'human rights news report',
        'press freedom news index', 'corruption news investigation', 'terrorism news security',
        'peacekeeping news mission', 'development aid news program', 'space cooperation news international',
        'cyber warfare news attack', 'espionage news intelligence', 'world economy news growth',
    ],
    'news_politics': [
        'political news today analysis', 'election news campaign update', 'Congress news legislation',
        'White House news announcement', 'Supreme Court news ruling', 'governor news state policy',
        'mayor news city policy', 'political party news strategy', 'primary election news result',
        'debate news political candidates', 'poll numbers news survey', 'campaign finance news donation',
        'political scandal news investigation', 'bipartisan news agreement deal', 'executive order news policy',
        'cabinet appointment news confirmation', 'ambassador news diplomatic post', 'impeachment news process',
        'redistricting news gerrymandering', 'voting rights news access', 'lobby news influence policy',
        'think tank news policy paper', 'political commentary news opinion', 'fact check news claim',
        'local politics news council', 'state legislature news bill', 'federal agency news regulation',
        'political protest news rally', 'political satire news comedy', 'political book news release',
    ],
    'news_environment': [
        'environment news today policy', 'climate change news report', 'renewable energy news project',
        'deforestation news Amazon', 'ocean pollution news plastic', 'air quality news city',
        'water crisis news drought', 'wildlife conservation news species', 'coral reef news bleaching',
        'glacier melting news arctic', 'wildfire news containment', 'flood news disaster response',
        'hurricane typhoon news track', 'earthquake news aftermath', 'volcano news eruption alert',
        'carbon emissions news reduction', 'green energy news investment', 'electric vehicle news adoption',
        'recycling news program policy', 'zero waste news initiative', 'sustainable agriculture news',
        'organic farming news growth', 'pesticide news regulation ban', 'biodiversity news decline',
        'endangered species news protection', 'national park news conservation', 'environmental law news ruling',
        'green building news standard', 'environmental justice news community', 'climate activist news movement',
    ],

    # ──────────────────────────────────────────────────────────────────
    # SHOPPING variants (30 queries each)
    # ──────────────────────────────────────────────────────────────────
    'shopping_electronics': [
        'best smartphones deals', 'laptop deals sale today', 'tablet best price comparison',
        'headphones on sale', 'smart TV deals this week', 'gaming console deals',
        'camera deals photography', 'smartwatch on sale', 'wireless earbuds deals',
        'speaker deals bluetooth', 'monitor deals gaming work', 'keyboard on sale mechanical',
        'mouse deals ergonomic', 'webcam deals HD quality', 'microphone deals podcast',
        'drone deals sale price', 'e-reader deals Kindle', 'projector deals home theater',
        'SSD deals storage sale', 'power bank deals charging', 'router deals wifi',
        'smart home devices deals', 'fitness tracker on sale', 'VR headset deals',
        'external hard drive deals', 'USB hub deals dock', 'charger deals fast charging',
        'cable organizer deals', 'phone case deals popular', 'screen protector deals',
    ],
    'shopping_clothing': [
        'women clothing sale online', 'men clothing deals today', 'kids clothing sale clearance',
        'dress sale formal casual', 'jeans deals popular brands', 'T-shirt sale graphic plain',
        'jacket coat sale winter', 'sweater hoodie deals cozy', 'activewear sale workout',
        'swimwear sale summer', 'underwear deals comfortable', 'socks deals quality',
        'shoes sale sneakers boots', 'sandals deals summer footwear', 'accessories sale bags belts',
        'formal wear sale suits', 'casual wear deals everyday', 'vintage clothing finds online',
        'sustainable clothing brands sale', 'plus size clothing deals', 'petite clothing sale',
        'tall size clothing deals', 'maternity clothing sale', 'designer clothing on sale',
        'fast fashion deals haul', 'workwear professional clothing sale', 'loungewear pajamas deals',
        'outerwear rain gear sale', 'sports apparel team gear sale', 'costume outfit themed sale',
    ],
    'shopping_compare': [
        'product comparison tool online', 'price comparison shopping engine', 'best deal finder app',
        'laptop comparison side by side', 'smartphone comparison specs price', 'tablet comparison features',
        'headphones comparison sound quality', 'TV comparison screen size price', 'camera comparison features price',
        'smartwatch comparison features battery', 'appliance comparison energy efficiency', 'mattress comparison comfort',
        'car seat comparison safety rating', 'stroller comparison features price', 'vacuum comparison suction price',
        'air purifier comparison room size', 'coffee maker comparison brew type', 'blender comparison power speed',
        'lawn mower comparison type price', 'tool set comparison quality price', 'insurance comparison quotes',
        'credit card comparison rewards', 'bank account comparison fees', 'streaming service comparison price',
        'VPN comparison speed privacy', 'web hosting comparison price features', 'meal kit comparison price menu',
        'subscription box comparison value', 'gym membership comparison price', 'cell phone plan comparison data',
    ],
    'shopping_deals': [
        'best deals today online shopping', 'Amazon deals today lightning', 'daily deals website check',
        'clearance sale online stores', 'flash sale happening now', 'coupon codes active today',
        'promo code free shipping', 'cashback deals today earn', 'buy one get one deals',
        'student discount deals online', 'military discount deals today', 'senior discount deals available',
        'bundle deals save money', 'refurbished deals electronics', 'open box deals near me',
        'warehouse deals discounted', 'seasonal sale deals check', 'holiday deals upcoming',
        'back to school deals August', 'end of year clearance deals', 'new year deals January',
        'Valentine deals gifts', 'Mother\'s Day deals gifts', 'Father\'s Day deals gifts',
        'Prime Day deals preview', 'Black Friday deals early', 'Cyber Monday deals tech',
        'Labor Day deals weekend', 'Memorial Day deals weekend', 'Fourth of July sale deals',
    ],
    'shopping_home': [
        'home decor sale online', 'furniture deals today', 'bedding sheets sale quality',
        'kitchen appliances deals sale', 'bathroom accessories sale', 'lighting fixtures on sale',
        'rugs carpets sale area', 'curtains blinds window sale', 'storage organization deals',
        'wall art decor sale prints', 'mirrors sale decorative', 'candles home fragrance sale',
        'plant pots planters sale', 'outdoor furniture patio sale', 'garden tools sale',
        'cleaning supplies deals', 'laundry supplies sale', 'trash cans recycling sale',
        'closet organizers sale', 'shelving units sale', 'desk office furniture sale',
        'chair cushion pillow sale', 'table runner placemats sale', 'vase centerpiece decor sale',
        'clock wall shelf sale', 'towels bath set sale', 'kitchen gadgets utensils sale',
        'cookware pots pans sale', 'dinnerware plates set sale', 'glassware drinkware sale',
    ],
    'shopping_beauty': [
        'skincare products sale deals', 'makeup deals popular brands', 'haircare products sale',
        'fragrance perfume deals today', 'nail care products sale', 'beauty tools deals sale',
        'organic beauty products sale', 'men grooming products deals', 'beauty gift sets sale',
        'sunscreen deals SPF products', 'anti aging products sale', 'acne treatment products deals',
        'hair styling tools sale', 'electric razor deals', 'beauty subscription box deals',
        'K-beauty products sale', 'luxury beauty deals discount', 'drugstore beauty deals today',
        'clean beauty products sale', 'body care lotion sale', 'lip care products deals',
        'eye care beauty products sale', 'bath bombs body wash sale', 'essential oils aromatherapy deals',
        'beauty appliance deals facial', 'teeth whitening products sale', 'deodorant deals natural',
        'shaving supplies deals', 'beauty advent calendar deal', 'travel size beauty products sale',
    ],
    'shopping_sports': [
        'sports equipment deals sale', 'running shoes deals today', 'yoga mat equipment sale',
        'gym equipment home deals', 'cycling gear deals bikes', 'camping gear sale outdoor',
        'fishing tackle equipment deals', 'hiking boots gear sale', 'tennis racket deals equipment',
        'golf clubs equipment sale', 'basketball gear shoes sale', 'soccer cleats ball deals',
        'swimming gear goggles sale', 'boxing gloves equipment deals', 'skateboard gear sale',
        'surfboard wetsuit deals', 'skiing snowboard gear sale', 'rock climbing gear deals',
        'martial arts equipment sale', 'fitness tracker wearable deals', 'sports nutrition supplement sale',
        'water bottle sports deals', 'sports bag backpack sale', 'compression wear athletic deals',
        'sports recovery equipment sale', 'sports sunglasses deals', 'sports watch deals GPS',
        'team sports uniforms deals', 'sports protective gear sale', 'jump rope resistance band deals',
    ],
    'shopping_toys': [
        'toys on sale popular kids', 'educational toys deals children', 'LEGO sets deals sale',
        'board games sale family', 'puzzle deals brain teaser', 'action figures deals popular',
        'dolls accessories deals sale', 'remote control toys sale', 'outdoor toys deals yard',
        'building blocks deals creative', 'arts crafts supplies kids sale', 'science kit deals educational',
        'musical toys instruments kids', 'stuffed animals plush deals', 'toy vehicles cars trucks sale',
        'pretend play toys kitchen sale', 'baby toys developmental deals', 'toddler toys learning sale',
        'gaming toys video game deals', 'collectible toys figurine deals', 'toy storage organizer deals',
        'birthday gift toys ideas sale', 'holiday gift toys popular sale', 'toy deals clearance',
        'wooden toys natural deals', 'electronic toys interactive sale', 'water toys pool summer sale',
        'toy brand deals popular kids', 'toy subscription box deals', 'used toys resale marketplace',
    ],
    'shopping_books': [
        'books on sale bestseller', 'ebook deals kindle today', 'audiobook deals sale listening',
        'fiction books sale popular', 'nonfiction books deals educational', 'children books sale illustrated',
        'textbook deals student semester', 'cookbook deals recipe collection', 'self help books sale',
        'business books deals leadership', 'science books sale popular', 'history books deals stories',
        'biography memoir books sale', 'mystery thriller books deals', 'romance books sale new',
        'fantasy sci fi books deals', 'graphic novel comic deals', 'art photography books sale',
        'travel guidebook deals planning', 'language learning books deals', 'religious spiritual books sale',
        'poetry books deals collection', 'coloring books adult deals', 'journal diary planner deals',
        'rare book finds online deals', 'used books cheap deals online', 'book bundle box set deals',
        'signed edition books special deals', 'book of the month club deal', 'library book sale near me',
    ],
    'shopping_garden': [
        'garden tools sale deals', 'plant seeds bulbs sale', 'flower pots planters deals',
        'garden soil compost sale', 'fertilizer plant food deals', 'garden hose nozzle sale',
        'lawn mower deals sale', 'trimmer edger deals garden', 'leaf blower deals fall',
        'garden furniture outdoor sale', 'fire pit outdoor deals', 'patio umbrella shade sale',
        'outdoor lighting garden deals', 'bird feeder bath sale', 'garden decor statue sale',
        'raised bed garden kit deals', 'greenhouse small portable deals', 'irrigation system drip sale',
        'mulch landscape supply deals', 'weed control products sale', 'pest control garden safe deals',
        'herb garden kit indoor sale', 'succulent cactus plant deals', 'tree shrub plant sale',
        'garden gloves tools set deals', 'wheelbarrow cart garden deals', 'compost bin tumbler sale',
        'garden kneeler pad deals', 'plant support trellis sale', 'garden storage shed deals',
    ],
    'shopping_automotive': [
        'car accessories deals sale', 'car floor mats deals', 'car seat covers sale',
        'dash cam deals best price', 'car phone mount deals', 'tire deals sale nearby',
        'car battery deals brand', 'motor oil deals synthetic', 'car cleaning products sale',
        'car wax polish deals', 'air freshener car deals', 'car charger USB deals',
        'car cover deals protection', 'car organizer storage deals', 'windshield wipers deals',
        'brake pads deals replacement', 'headlight bulbs deals bright', 'car jack stands deals',
        'jumper cables deals quality', 'tire pressure gauge deals', 'car tool kit deals essential',
        'paint touch up car deals', 'car stereo deals upgrade', 'car speaker deals audio',
        'GPS navigation deals car', 'radar detector deals legal', 'car alarm deals security',
        'roof rack cargo deals', 'trailer hitch deals towing', 'car emergency kit deals',
    ],
    'shopping_jewelry': [
        'jewelry deals sale online', 'necklace deals pendant chain', 'earrings deals studs hoops',
        'bracelet deals bangles cuffs', 'ring deals engagement fashion', 'watch deals luxury affordable',
        'diamond jewelry deals sale', 'gold jewelry deals karat', 'silver jewelry deals sterling',
        'gemstone jewelry deals colorful', 'pearl jewelry deals classic', 'costume jewelry deals fashion',
        'men jewelry deals watches rings', 'kids jewelry deals cute safe', 'personalized jewelry deals custom',
        'wedding band deals matching', 'anniversary gift jewelry deals', 'birthday gift jewelry deals',
        'Mother\'s Day jewelry deals', 'Valentine jewelry deals romantic', 'graduation gift jewelry deals',
        'handmade artisan jewelry deals', 'vintage antique jewelry deals', 'designer brand jewelry sale',
        'jewelry box organizer deals', 'jewelry cleaning care deals', 'charm bracelet beads deals',
        'body jewelry piercing deals', 'hair jewelry accessories deals', 'brooch pin jewelry deals',
    ],
}

# French-specific queries for ALL search activity categories
# Used when country='FR' — ensures realistic French user browsing behavior
ACTIVITY_QUERIES_FR = {
    # Core searches
    'search_restaurants': ['meilleurs restaurants paris', 'restaurant gastronomique pas cher', 'restaurant livraison domicile', 'meilleur burger paris', 'restaurant végétarien lyon', 'pizza livraison rapide', 'restaurant romantique bordeaux', 'avis tripadvisor restaurant', 'menu du jour restaurant', 'restaurant ouvert dimanche'],
    'search_news':        ['actualités france aujourd\'hui', 'dernières nouvelles france', 'actu politique france', 'journal télévisé replay', 'infos locales', 'le monde actualités', 'lefigaro actualités', 'bfmtv dernières nouvelles', 'actualités économie france', 'actu régionale'],
    'search_weather':     ['météo aujourd\'hui paris', 'météo demain lyon', 'prévisions météo semaine', 'météo ile de france', 'météo côte d\'azur', 'températures france', 'météo montagne', 'météo bordeaux', 'météo marseille', 'météo week-end'],
    'search_movies':      ['films français 2025', 'meilleurs films netflix france', 'cinéma programme cette semaine', 'films à voir 2025', 'allocine avis films', 'séries netflix populaires', 'films d\'action 2025', 'comédie française récente', 'oscars 2025 films', 'disney plus nouveautés'],
    'search_sports':      ['résultats ligue 1 aujourd\'hui', 'classement ligue 1 2025', 'psg match prochain', 'équipe de france football', 'tour de france 2025', 'roland garros résultats', 'champions league programme', 'rugby top 14', 'transfert foot france', 'tennis atp résultats'],
    'search_tech':        ['meilleur smartphone 2025', 'test iphone 16 pro', 'samsung galaxy comparatif', 'meilleur ordinateur portable', 'tablette ipad ou samsung', 'vrai test airpods', 'smart tv guide d\'achat', 'comparatif antivirus 2025', 'clé usb vitesse rapide', 'écouteurs sans fil test'],
    'search_travel':      ['voyage pas cher europe', 'destination vacances été 2025', 'vol paris rome pas cher', 'location voiture vacances', 'camping france bord mer', 'hôtels pas chers paris', 'visa espagne français', 'que faire à amsterdam', 'week-end romantique france', 'train pas cher sncf'],
    'search_recipes':     ['recette crêpes bretonnes', 'recette tarte tatin facile', 'poulet rôti recette', 'gratin dauphinois maison', 'quiche lorraine originale', 'soupe à l\'oignon gratinée', 'boeuf bourguignon recette', 'crème brûlée maison', 'ratatouille provençale', 'recette pain maison levure'],
    'search_products':    ['avis produits amazon france', 'comparatif aspirateur robot', 'meilleur robot cuisine', 'cafetière capsule test', 'tondeuse barbe avis', 'machine à café comparatif', 'aspirateur dyson test', 'friteuse sans huile comparatif', 'climatiseur mobile test', 'purificateur air comparatif'],
    'search_local':       ['pharmacie de garde près de moi', 'médecin généraliste disponible', 'supermarché ouvert maintenant', 'coiffeur pas cher près de moi', 'garage auto réparation', 'boulangerie artisanale', 'pressing à proximité', 'opticien paris', 'dentiste urgence', 'serrurier urgence'],
    # Extended categories
    'search_crypto':      ['prix bitcoin aujourd\'hui', 'cours ethereum en direct', 'crypto actualités france', 'meilleure crypto à acheter', 'portefeuille crypto application', 'cryptomonnaie débutant', 'ripple xrp cours', 'avalanche crypto prix', 'defi finance décentralisée', 'nft marché 2025'],
    'search_fashion':     ['tendances mode femme 2025', 'marques mode française luxe', 'tenues printemps été', 'mode homme casual chic', 'soldes été france', 'vêtements bio éthique', 'style vestimentaire parisien', 'sacs à main tendance', 'chaussures mode 2025', 'comment s\'habiller élégamment'],
    'search_beauty':      ['routine beauté matin naturelle', 'meilleur soin hydratant visage', 'tutoriel maquillage naturel', 'soin cheveux bouclés', 'produits beauté bio france', 'anti-âge crème efficace', 'fond de teint teint parfait', 'soin peau sensible', 'routine cheveux abîmés', 'cosmétiques français bio'],
    'search_jobs':        ['offres emploi télétravail france', 'comment rédiger cv 2025', 'emploi cadre paris', 'métiers bien rémunérés', 'freelance indépendant france', 'pole emploi offres', 'reconversion professionnelle', 'lettre motivation exemple', 'entretien embauche conseils', 'salaire moyen france 2025'],
    'search_finance':     ['comment investir son argent', 'bourse cac40 aujourd\'hui', 'taux livret a 2025', 'crédit immobilier taux', 'retraite épargne conseils', 'assurance vie avantages', 'impôts déclaration revenus', 'bourse débutant france', 'meilleure banque en ligne', 'actions dividendes france'],
    'search_fitness':     ['exercices maison sans matériel', 'perdre du ventre rapidement', 'aliments riches en protéines', 'yoga débutant matin', 'programme musculation gratuit', 'cardio brûle graisses', 'stretching quotidien', 'courir débutant programme', 'nutrition sportive conseils', 'hiit entraînement maison'],
    'search_health':      ['symptômes carence vitamine d', 'améliorer sommeil naturellement', 'gestion stress quotidien', 'booster immunité naturellement', 'compléments alimentaires avis', 'santé mentale conseils', 'méditation bienfaits', 'alimentation équilibrée semaine', 'remèdes naturels rhume', 'douleurs dos soulager'],
    'search_realestate':  ['prix immobilier paris 2025', 'louer appartement pas cher', 'acheter maison france', 'crédit immobilier simulation', 'location saisonnière airbnb', 'investissement locatif rentable', 'dpe maison explication', 'terrain constructible prix', 'promoteur immobilier avis', 'notaire frais achat immobilier'],
    'search_gaming':      ['meilleurs jeux pc 2025', 'ps5 jeux exclusifs', 'jeux switch nouveautés', 'gaming setup budget france', 'jeux gratuits 2025', 'gta 6 sortie france', 'minecraft serveur français', 'league of legends guide', 'steam soldes jeux', 'jeux de rôle rpg pc'],
    'search_diy':         ['bricolage maison débutant', 'comment peindre mur salon', 'renovation appartement budget', 'jardinage potager débutant', 'décoration intérieure tendance', 'parquet poser soi-même', 'plomberie fuite réparer', 'menuiserie bois débutant', 'isolation maison économie', 'cuisine relooker pas cher'],
    'search_cars':        ['meilleure voiture électrique 2025', 'voiture d\'occasion achat conseils', 'contrôle technique obligation', 'assurance auto pas chère', 'entretien voiture soi-même', 'pneus quand changer', 'bonus écologique voiture', 'comparatif suv 2025', 'leasing voiture avantages', 'voiture hybride rechargeable'],
    'search_pets':        ['alimentation chien naturelle', 'meilleure nourriture chat', 'vétérinaire urgence paris', 'dresser son chien maison', 'race chien appartement', 'vaccination chien calendrier', 'comportement chat comprendre', 'assurance animaux comparatif', 'aquarium débutant poissons', 'lapin nain soins'],
    'search_education':   ['cours en ligne gratuit france', 'apprendre anglais rapidement', 'formation professionnelle gratuite', 'bac révisions conseils', 'université france inscription', 'mooc certificat reconnu', 'formation certifiante en ligne', 'apprendre programmation python', 'parcoursup orientation', 'bourse étudiant conditions'],
    'search_shopping':    ['code promo amazon france', 'soldes été bonnes affaires', 'cashback application france', 'fnac promo du jour', 'vinted occasion mode', 'comparateur prix en ligne', 'black friday france date', 'leboncoin achat vente', 'cdiscount promotions', 'veepee ventes privées'],
    'search_tech_news':   ['actualités technologie france', 'intelligence artificielle actualités', 'nouvelles applications 2025', 'cybersécurité conseils', 'apple nouveautés 2025', 'google android mise à jour', 'nouvelles voitures électriques tech', 'maison connectée domotique', 'réseaux sociaux actualités', 'startup française tech'],
    'search_sports_news': ['ligue 1 résultats', 'psg champions league', 'rugby xv de france', 'tour de france étapes', 'tennis roland garros', 'formule 1 grand prix', 'équipe france résultats', 'handball france championnat', 'jeux olympiques france', 'cyclisme france actualités'],
    'search_news_world':  ['actualités monde aujourd\'hui', 'europe politique actualités', 'économie mondiale 2025', 'climat environnement france', 'conflit international news', 'relations diplomatiques france', 'sciences découvertes', 'espace exploration nouvelles', 'santé actualités mondiales', 'technologie monde'],
    'search_shopping_electronics': ['bon plan high tech france', 'promotion téléphone', 'pc portable soldes', 'tablette prix réduit', 'écran pc offre', 'casque audio promo', 'montre connectée soldes', 'appareil photo offre', 'enceinte bluetooth promo', 'disque dur externe pas cher'],
}

# Map: country code → activity query dict
# Add more countries here to expand localization
_ACTIVITY_QUERIES_BY_COUNTRY = {
    'FR': ACTIVITY_QUERIES_FR,
    # Add 'DE', 'ES', 'IT' etc. here when needed
}

ACTIVITY_MAP = {
    # Google Search
    'search_restaurants':   (_google_search, 'restaurants'),
    'search_news':          (_google_search, 'news'),
    'search_weather':       (_google_search, 'weather'),
    'search_movies':        (_google_search, 'movies'),
    'search_sports':        (_google_search, 'sports'),
    'search_tech':          (_google_search, 'tech'),
    'search_travel':        (_google_search, 'travel'),
    'search_recipes':       (_google_search, 'recipes'),
    'search_products':      (_google_search, 'products'),
    'search_local':         (_google_search, 'local'),
    # Gmail
    'gmail_inbox':          (_check_gmail, 'inbox'),
    'gmail_read_email':     (_check_gmail, 'read'),
    'gmail_check_spam':     (_check_gmail, 'spam'),
    'gmail_check_sent':     (_check_gmail, 'sent'),
    'gmail_scroll_inbox':   (_check_gmail, 'scroll'),
    'gmail_search_email':   (_check_gmail, 'search'),
    # YouTube
    'youtube_browse_feed':  (_browse_youtube, 'feed'),
    'youtube_trending':     (_browse_youtube, 'trending'),
    'youtube_search_music': (_browse_youtube, 'music'),
    'youtube_shorts':       (_browse_youtube, 'shorts'),
    'youtube_subscriptions':(_browse_youtube, 'subscriptions'),
    'youtube_watch_video':  (_browse_youtube, 'watch'),
    # Google Maps
    'maps_search_restaurants': (_browse_maps, 'restaurants'),
    'maps_directions':      (_browse_maps, 'directions'),
    'maps_browse_places':   (_browse_maps, 'places'),
    'maps_view_photos':     (_browse_maps, 'photos'),
    'maps_read_reviews':    (_browse_maps, 'reviews'),
    'maps_street_view':     (_browse_maps, 'street'),
    # Google Drive
    'drive_browse':         (_visit_drive, 'browse'),
    'drive_recent':         (_visit_drive, 'recent'),
    'drive_shared':         (_visit_drive, 'shared'),
    'drive_new_doc':        (_visit_drive, 'new_doc'),
    # Google Account
    'account_security':     (_visit_account, 'security'),
    'account_activity':     (_visit_account, 'activity'),
    'account_profile':      (_visit_account, 'profile'),
    'account_privacy':      (_visit_account, 'privacy'),
    # Google News
    'news_headlines':       (_browse_news, 'headlines'),
    'news_tech':            (_browse_news, 'tech'),
    'news_sports':          (_browse_news, 'sports'),
    'news_entertainment':   (_browse_news, 'entertainment'),
    'news_business':        (_browse_news, 'business'),
    # Google Shopping
    'shopping_electronics': (_browse_shopping, 'electronics'),
    'shopping_clothing':    (_browse_shopping, 'clothing'),
    'shopping_compare':     (_browse_shopping, 'compare'),
    'shopping_deals':       (_browse_shopping, 'deals'),
    # Google Photos
    'photos_browse':        (_visit_photos, 'browse'),
    'photos_albums':        (_visit_photos, 'albums'),
    # Other Services
    'translate_phrases':    (_visit_translate, ''),
    'calendar_view':        (_visit_calendar, ''),
    'keep_browse':          (_visit_keep, ''),
    'play_browse_apps':     (_visit_play, ''),
    'meet_check':           (_visit_meet, ''),
    # Specific topic searches
    'search_crypto':      (_google_search, 'crypto'),
    'search_fashion':     (_google_search, 'fashion'),
    'search_beauty':      (_google_search, 'beauty'),
    'search_jobs':        (_google_search, 'jobs'),
    'search_finance':     (_google_search, 'finance'),
    'search_fitness':     (_google_search, 'fitness'),
    'search_health':      (_google_search, 'health'),
    'search_realestate':  (_google_search, 'realestate'),
    'search_gaming':      (_google_search, 'gaming'),
    'search_diy':         (_google_search, 'diy'),
    'search_cars':        (_google_search, 'cars'),
    'search_pets':        (_google_search, 'pets'),
    'search_education':   (_google_search, 'education'),
    'search_food':        (_google_search, 'food'),
    'search_shopping':    (_google_search, 'shopping'),
    'search_tech_news':   (_google_search, 'tech_news'),
    'search_sports_news': (_google_search, 'sports_news'),
    'search_news_world':  (_google_search, 'news_world'),
    # --- New Google Search activities ---
    'search_music':          (_google_search, 'music'),
    'search_gardening':      (_google_search, 'gardening'),
    'search_photography':    (_google_search, 'photography'),
    'search_astronomy':      (_google_search, 'astronomy'),
    'search_history':        (_google_search, 'history'),
    'search_science':        (_google_search, 'science'),
    'search_books':          (_google_search, 'books'),
    'search_art':            (_google_search, 'art'),
    'search_cooking':        (_google_search, 'cooking'),
    'search_parenting':      (_google_search, 'parenting'),
    'search_psychology':     (_google_search, 'psychology'),
    'search_philosophy':     (_google_search, 'philosophy'),
    'search_architecture':   (_google_search, 'architecture'),
    'search_interior_design':(_google_search, 'interior_design'),
    'search_marketing':      (_google_search, 'marketing'),
    'search_startups':       (_google_search, 'startups'),
    'search_programming':    (_google_search, 'programming'),
    'search_ai':             (_google_search, 'ai'),
    'search_space':          (_google_search, 'space'),
    'search_environment':    (_google_search, 'environment'),
    'search_politics':       (_google_search, 'politics'),
    'search_comedy':         (_google_search, 'comedy'),
    'search_podcasts':       (_google_search, 'podcasts'),
    'search_documentaries':  (_google_search, 'documentaries'),
    'search_anime':          (_google_search, 'anime'),
    'search_manga':          (_google_search, 'manga'),
    'search_board_games':    (_google_search, 'board_games'),
    'search_camping':        (_google_search, 'camping'),
    'search_fishing':        (_google_search, 'fishing'),
    'search_cycling':        (_google_search, 'cycling'),
    # --- New Gmail activities ---
    'gmail_compose_draft':   (_check_gmail, 'compose_draft'),
    'gmail_star_email':      (_check_gmail, 'star'),
    'gmail_labels':          (_check_gmail, 'labels'),
    'gmail_contacts':        (_check_gmail, 'contacts'),
    # --- New YouTube activities ---
    'youtube_gaming':        (_browse_youtube, 'gaming'),
    'youtube_cooking':       (_browse_youtube, 'cooking'),
    'youtube_news':          (_browse_youtube, 'news'),
    'youtube_education':     (_browse_youtube, 'education'),
    'youtube_tech_review':   (_browse_youtube, 'tech_review'),
    'youtube_vlogs':         (_browse_youtube, 'vlogs'),
    'youtube_sports':        (_browse_youtube, 'sports'),
    'youtube_music_playlist':(_browse_youtube, 'music_playlist'),
    'youtube_diy':           (_browse_youtube, 'diy'),
    'youtube_travel':        (_browse_youtube, 'travel'),
    'youtube_fitness':       (_browse_youtube, 'fitness'),
    'youtube_comedy':        (_browse_youtube, 'comedy'),
    'youtube_documentary':   (_browse_youtube, 'documentary'),
    'youtube_asmr':          (_browse_youtube, 'asmr'),
    # --- New Maps activities ---
    'maps_gas_stations':     (_browse_maps, 'gas_stations'),
    'maps_hotels':           (_browse_maps, 'hotels'),
    'maps_hospitals':        (_browse_maps, 'hospitals'),
    'maps_parks':            (_browse_maps, 'parks'),
    'maps_shopping_malls':   (_browse_maps, 'shopping_malls'),
    'maps_coffee_shops':     (_browse_maps, 'coffee_shops'),
    'maps_gyms':             (_browse_maps, 'gyms'),
    'maps_atms':             (_browse_maps, 'atms'),
    'maps_pharmacies':       (_browse_maps, 'pharmacies'),
    'maps_schools':          (_browse_maps, 'schools'),
    'maps_banks':            (_browse_maps, 'banks'),
    'maps_supermarkets':     (_browse_maps, 'supermarkets'),
    'maps_car_wash':         (_browse_maps, 'car_wash'),
    'maps_libraries':        (_browse_maps, 'libraries'),
    'maps_museums':          (_browse_maps, 'museums'),
    'maps_theaters':         (_browse_maps, 'theaters'),
    # --- New News activities ---
    'news_health':           (_browse_news, 'health'),
    'news_science':          (_browse_news, 'science'),
    'news_world':            (_browse_news, 'world'),
    'news_politics':         (_browse_news, 'politics'),
    'news_environment':      (_browse_news, 'environment'),
    'news_education':        (_browse_news, 'education'),
    # --- New Shopping activities ---
    'shopping_home':         (_browse_shopping, 'home'),
    'shopping_beauty':       (_browse_shopping, 'beauty'),
    'shopping_sports':       (_browse_shopping, 'sports'),
    'shopping_toys':         (_browse_shopping, 'toys'),
    'shopping_books':        (_browse_shopping, 'books'),
    'shopping_garden':       (_browse_shopping, 'garden'),
    'shopping_automotive':   (_browse_shopping, 'automotive'),
    'shopping_jewelry':      (_browse_shopping, 'jewelry'),
    # --- New Drive activities ---
    'drive_search':          (_visit_drive, 'search'),
    'drive_trash':           (_visit_drive, 'trash'),
    'drive_starred':         (_visit_drive, 'starred'),
    # --- New Account activities ---
    'account_storage':       (_visit_account, 'storage'),
    'account_payments':      (_visit_account, 'payments'),
    'account_data_export':   (_visit_account, 'data_export'),
    # --- New Photos activities ---
    'photos_favorites':      (_visit_photos, 'favorites'),
    'photos_archive':        (_visit_photos, 'archive'),
    'photos_trash':          (_visit_photos, 'trash'),
    # --- New Other Services ---
    'contacts_browse':       (_visit_contacts, 'browse'),
    'contacts_search':       (_visit_contacts, 'search'),
    'blogger_browse':        (_visit_blogger, 'browse'),
    'sites_browse':          (_visit_sites, 'browse'),
    'forms_browse':          (_visit_forms, 'browse'),
    # --- Custom GMB ---
    'custom_gmb':            (_custom_gmb_activity, 'gmb'),
}


async def gmail_health_activity(page, worker_id, duration_minutes=0, country='US',
                                activities=None, rounds=1,
                                gmb_name='', gmb_address=''):
    """
    Run random human-like activities.

    Duration is a HARD LIMIT — activities stop when time runs out.
    If duration=0, runs all activities x rounds then stops.

    Args:
        page: Playwright page object (logged-in Google profile)
        worker_id: Worker number for logging
        duration_minutes: HARD time limit in minutes. 0 = no limit (finish activities then stop).
        country: Country code for localized queries
        activities: List of activity IDs to run. If None, uses defaults.
        rounds: Repeat activity list N times (ignored if duration reached first).
        gmb_name: Business name for GMB-aware queries.
        gmb_address: Business address for GMB-aware queries.
    """
    if not activities:
        activities = ['search_restaurants', 'search_news', 'gmail_inbox',
                      'youtube_browse_feed', 'maps_search_restaurants', 'news_headlines']

    has_time_limit = duration_minutes > 0
    end_time = time.time() + (duration_minutes * 60) if has_time_limit else 0
    start_time = time.time()

    _log(worker_id, f"[HEALTH] Starting — {len(activities)} activities x {rounds} round(s), "
         f"duration={'%d min' % duration_minutes if has_time_limit else 'unlimited'}, country={country}")

    activities_done = 0
    activity_log = []

    def _time_left():
        return (end_time - time.time()) if has_time_limit else 999

    def _time_up():
        return has_time_limit and time.time() >= end_time

    async def _run_one(act_id):
        """Run a single activity. Returns True if successful."""
        nonlocal activities_done
        entry = ACTIVITY_MAP.get(act_id)
        if not entry:
            return False
        fn, variant = entry
        try:
            # Resolve query from pools
            specific_q = None
            country_queries = _ACTIVITY_QUERIES_BY_COUNTRY.get(country)
            if country_queries and act_id in country_queries:
                specific_q = random.choice(country_queries[act_id])
            elif act_id in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[act_id])
            elif variant in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[variant])
            elif fn == _browse_youtube and f'yt_{variant}' in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[f'yt_{variant}'])
            elif fn == _browse_maps and f'maps_{variant}' in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[f'maps_{variant}'])
            elif fn == _browse_news and f'news_{variant}' in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[f'news_{variant}'])
            elif fn == _browse_shopping and f'shopping_{variant}' in ACTIVITY_QUERIES:
                specific_q = random.choice(ACTIVITY_QUERIES[f'shopping_{variant}'])

            if fn == _custom_gmb_activity:
                await fn(page, worker_id, gmb_name=gmb_name, gmb_address=gmb_address, country=country)
            else:
                kwargs = dict(country=country, gmb_name=gmb_name, gmb_address=gmb_address)
                if specific_q is not None:
                    kwargs['query'] = specific_q
                await fn(page, worker_id, **kwargs)
            activities_done += 1
            activity_log.append(act_id)
            return True
        except Exception as e:
            _log(worker_id, f"[HEALTH] {act_id} error: {e}")
            return False

    # ── Main loop: rounds with hard time check ──
    for round_num in range(rounds):
        if _time_up():
            _log(worker_id, f"[HEALTH] Time limit reached before round {round_num + 1}")
            break

        _log(worker_id, f"[HEALTH] === Round {round_num + 1}/{rounds} === ({_time_left():.0f}s left)" if has_time_limit
             else f"[HEALTH] === Round {round_num + 1}/{rounds} ===")

        shuffled = list(activities)
        random.shuffle(shuffled)

        for act_id in shuffled:
            if _time_up():
                _log(worker_id, f"[HEALTH] Duration {duration_minutes}min reached — stopping")
                break
            _log(worker_id, f"[HEALTH] Activity {activities_done + 1}: {act_id}")
            await _run_one(act_id)
            # Short pause (3-8s) to stay within time budget
            await asyncio.sleep(random.uniform(3.0, 8.0))

        if _time_up():
            break

        if round_num < rounds - 1 and not _time_up():
            pause = random.uniform(5.0, 15.0)
            _log(worker_id, f"[HEALTH] Round pause {pause:.0f}s")
            await asyncio.sleep(pause)

    # ── Time-fill: if duration set and time remaining, keep going ──
    if has_time_limit and not _time_up():
        _log(worker_id, f"[HEALTH] Time-fill: {_time_left():.0f}s remaining, picking random activities...")
        fill_pool = list(ACTIVITY_MAP.keys())
        while not _time_up():
            act_id = random.choice(fill_pool)
            await _run_one(act_id)
            await asyncio.sleep(random.uniform(3.0, 8.0))

    elapsed = int(time.time() - start_time)
    _log(worker_id, f"[HEALTH] Complete — {activities_done} activities in {elapsed // 60}m {elapsed % 60}s")
    return {
        'success': True,
        'activities_done': activities_done,
        'activity_log': activity_log,
    }
