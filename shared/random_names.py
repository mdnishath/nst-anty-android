"""Random name generator by country for profile operations."""
import random

# Common first/last names by country
NAMES_BY_COUNTRY = {
    'US': {
        'first_male': ['James', 'John', 'Robert', 'Michael', 'David', 'William', 'Richard', 'Joseph', 'Thomas', 'Christopher', 'Daniel', 'Matthew', 'Anthony', 'Mark', 'Steven', 'Andrew', 'Brian', 'Joshua', 'Kevin', 'Ryan'],
        'first_female': ['Mary', 'Patricia', 'Jennifer', 'Linda', 'Barbara', 'Elizabeth', 'Susan', 'Jessica', 'Sarah', 'Karen', 'Lisa', 'Nancy', 'Betty', 'Margaret', 'Sandra', 'Ashley', 'Emily', 'Donna', 'Michelle', 'Dorothy'],
        'last': ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin'],
    },
    'UK': {
        'first_male': ['Oliver', 'George', 'Harry', 'Jack', 'Jacob', 'Noah', 'Charlie', 'Muhammad', 'Thomas', 'Oscar', 'William', 'James', 'Leo', 'Alfie', 'Henry', 'Archie', 'Edward', 'Samuel', 'Alexander', 'Daniel'],
        'first_female': ['Olivia', 'Amelia', 'Isla', 'Ava', 'Emily', 'Isabella', 'Mia', 'Poppy', 'Ella', 'Lily', 'Grace', 'Sophie', 'Daisy', 'Freya', 'Phoebe', 'Evie', 'Charlotte', 'Florence', 'Ruby', 'Rosie'],
        'last': ['Smith', 'Jones', 'Taylor', 'Brown', 'Williams', 'Wilson', 'Johnson', 'Davies', 'Robinson', 'Wright', 'Thompson', 'Evans', 'Walker', 'White', 'Roberts', 'Green', 'Hall', 'Wood', 'Jackson', 'Clarke'],
    },
    'BD': {
        'first_male': ['Mohammad', 'Abdul', 'Md', 'Syed', 'Ahmed', 'Rahim', 'Karim', 'Hasan', 'Hossain', 'Rahman', 'Alam', 'Islam', 'Uddin', 'Miah', 'Kabir', 'Nasir', 'Faruk', 'Jamal', 'Rafiq', 'Shahid'],
        'first_female': ['Fatima', 'Ayesha', 'Nusrat', 'Shamima', 'Rahima', 'Hasina', 'Nasreen', 'Sultana', 'Begum', 'Akter', 'Khatun', 'Jahan', 'Noor', 'Laila', 'Taslima', 'Momena', 'Salma', 'Razia', 'Amina', 'Dilara'],
        'last': ['Rahman', 'Islam', 'Hossain', 'Ahmed', 'Alam', 'Uddin', 'Miah', 'Khan', 'Chowdhury', 'Hasan', 'Ali', 'Kabir', 'Karim', 'Bhuiyan', 'Talukder', 'Sarkar', 'Siddique', 'Malik', 'Sheikh', 'Mahmud'],
    },
    'IN': {
        'first_male': ['Aarav', 'Vivaan', 'Aditya', 'Vihaan', 'Arjun', 'Sai', 'Reyansh', 'Ayaan', 'Krishna', 'Ishaan', 'Rohan', 'Rahul', 'Amit', 'Raj', 'Vikram', 'Suresh', 'Deepak', 'Nikhil', 'Ankit', 'Karan'],
        'first_female': ['Aadhya', 'Saanvi', 'Aanya', 'Ananya', 'Pari', 'Myra', 'Sara', 'Ira', 'Diya', 'Priya', 'Neha', 'Pooja', 'Sneha', 'Divya', 'Kavya', 'Riya', 'Tanvi', 'Anjali', 'Nisha', 'Meera'],
        'last': ['Sharma', 'Verma', 'Gupta', 'Singh', 'Kumar', 'Patel', 'Shah', 'Joshi', 'Reddy', 'Nair', 'Iyer', 'Mehta', 'Das', 'Mukherjee', 'Chatterjee', 'Pillai', 'Rao', 'Mishra', 'Chauhan', 'Agarwal'],
    },
    'DE': {
        'first_male': ['Lukas', 'Leon', 'Maximilian', 'Felix', 'Paul', 'Jonas', 'Tim', 'Elias', 'Finn', 'Noah', 'Liam', 'Ben', 'Niklas', 'Jan', 'Moritz', 'Julian', 'David', 'Erik', 'Fabian', 'Tobias'],
        'first_female': ['Emma', 'Mia', 'Hannah', 'Sofia', 'Anna', 'Lea', 'Lena', 'Marie', 'Johanna', 'Clara', 'Laura', 'Sarah', 'Lisa', 'Julia', 'Katharina', 'Nina', 'Lara', 'Maja', 'Amelie', 'Jana'],
        'last': ['Müller', 'Schmidt', 'Schneider', 'Fischer', 'Weber', 'Meyer', 'Wagner', 'Becker', 'Schulz', 'Hoffmann', 'Koch', 'Richter', 'Wolf', 'Klein', 'Schröder', 'Neumann', 'Schwarz', 'Braun', 'Zimmermann', 'Krüger'],
    },
    'FR': {
        'first_male': ['Gabriel', 'Raphaël', 'Léo', 'Louis', 'Lucas', 'Adam', 'Hugo', 'Arthur', 'Jules', 'Maël', 'Nathan', 'Ethan', 'Paul', 'Noah', 'Liam', 'Tom', 'Théo', 'Sacha', 'Mathis', 'Nolan'],
        'first_female': ['Emma', 'Jade', 'Louise', 'Alice', 'Chloé', 'Lina', 'Rose', 'Léa', 'Anna', 'Mila', 'Ambre', 'Julia', 'Manon', 'Camille', 'Zoé', 'Inès', 'Clara', 'Sarah', 'Margaux', 'Juliette'],
        'last': ['Martin', 'Bernard', 'Dubois', 'Thomas', 'Robert', 'Richard', 'Petit', 'Durand', 'Leroy', 'Moreau', 'Simon', 'Laurent', 'Lefebvre', 'Michel', 'Garcia', 'David', 'Bertrand', 'Roux', 'Vincent', 'Fournier'],
    },
    'BR': {
        'first_male': ['Miguel', 'Arthur', 'Heitor', 'Bernardo', 'Théo', 'Davi', 'Gabriel', 'Pedro', 'Samuel', 'Lorenzo', 'Lucas', 'Rafael', 'Matheus', 'Felipe', 'Gustavo', 'João', 'Carlos', 'Diego', 'Bruno', 'André'],
        'first_female': ['Alice', 'Sophia', 'Helena', 'Valentina', 'Laura', 'Isabella', 'Manuela', 'Júlia', 'Heloísa', 'Luísa', 'Maria', 'Ana', 'Beatriz', 'Gabriela', 'Fernanda', 'Letícia', 'Camila', 'Larissa', 'Carolina', 'Mariana'],
        'last': ['Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues', 'Ferreira', 'Alves', 'Pereira', 'Lima', 'Gomes', 'Costa', 'Ribeiro', 'Martins', 'Carvalho', 'Araújo', 'Melo', 'Barbosa', 'Cardoso', 'Nascimento', 'Moreira'],
    },
    'TR': {
        'first_male': ['Yusuf', 'Eymen', 'Ömer', 'Mustafa', 'Ali', 'Ahmet', 'Mehmet', 'Kerem', 'Miraç', 'Hamza', 'Burak', 'Emre', 'Serkan', 'Murat', 'Hakan', 'Oğuz', 'Berk', 'Can', 'Fatih', 'Efe'],
        'first_female': ['Zeynep', 'Elif', 'Defne', 'Ada', 'Ebrar', 'Hiranur', 'Azra', 'Asya', 'Eylül', 'Ecrin', 'Ayşe', 'Fatma', 'Merve', 'Büşra', 'Esra', 'Selin', 'Deniz', 'Naz', 'Melis', 'Beren'],
        'last': ['Yılmaz', 'Kaya', 'Demir', 'Çelik', 'Şahin', 'Yıldız', 'Yıldırım', 'Öztürk', 'Aydın', 'Özdemir', 'Arslan', 'Doğan', 'Kılıç', 'Aslan', 'Çetin', 'Kara', 'Koç', 'Kurt', 'Özkan', 'Erdoğan'],
    },
    'PK': {
        'first_male': ['Muhammad', 'Ahmed', 'Ali', 'Hassan', 'Hussain', 'Usman', 'Abdullah', 'Bilal', 'Imran', 'Hamza', 'Faisal', 'Zubair', 'Rizwan', 'Waqar', 'Shahid', 'Kamran', 'Asad', 'Saad', 'Tariq', 'Naveed'],
        'first_female': ['Fatima', 'Ayesha', 'Zainab', 'Maryam', 'Hira', 'Sana', 'Amna', 'Bushra', 'Rabia', 'Nadia', 'Sara', 'Khadija', 'Sadia', 'Noor', 'Alina', 'Iqra', 'Madiha', 'Farah', 'Samina', 'Rubina'],
        'last': ['Khan', 'Ahmed', 'Ali', 'Hussain', 'Malik', 'Butt', 'Sheikh', 'Iqbal', 'Siddiqui', 'Qureshi', 'Mirza', 'Chaudhry', 'Aslam', 'Raza', 'Rehman', 'Nawaz', 'Shah', 'Dar', 'Gill', 'Bhatti'],
    },
    'ID': {
        'first_male': ['Muhammad', 'Ahmad', 'Rizky', 'Dimas', 'Bayu', 'Adi', 'Fauzan', 'Yoga', 'Budi', 'Agus', 'Wahyu', 'Hadi', 'Rudi', 'Dian', 'Arif', 'Eko', 'Joko', 'Bambang', 'Sigit', 'Gunawan'],
        'first_female': ['Siti', 'Dewi', 'Putri', 'Sri', 'Rina', 'Ayu', 'Wulan', 'Maya', 'Indah', 'Fitri', 'Dian', 'Ratna', 'Ani', 'Eka', 'Mega', 'Rini', 'Lina', 'Yuni', 'Nisa', 'Sari'],
        'last': ['Pratama', 'Saputra', 'Hidayat', 'Kurniawan', 'Santoso', 'Nugroho', 'Wijaya', 'Putra', 'Setiawan', 'Utomo', 'Susanto', 'Gunawan', 'Budiman', 'Wibowo', 'Hartono', 'Surya', 'Handoko', 'Suryadi', 'Sutrisno', 'Prasetyo'],
    },
    'PH': {
        'first_male': ['James', 'John', 'Mark', 'Francis', 'Jose', 'Angelo', 'Christian', 'Miguel', 'Daniel', 'Joshua', 'Carlo', 'Rafael', 'Kenneth', 'Paul', 'Michael', 'Jerome', 'Kevin', 'Ryan', 'Patrick', 'Adrian'],
        'first_female': ['Maria', 'Ana', 'Grace', 'Joy', 'Rose', 'Princess', 'Angel', 'Nicole', 'Jasmine', 'Christine', 'Mary', 'Kate', 'Andrea', 'Ella', 'Sophia', 'Isabel', 'Patricia', 'Angelica', 'Krystal', 'Mae'],
        'last': ['Santos', 'Reyes', 'Cruz', 'Bautista', 'Garcia', 'Gonzales', 'Hernandez', 'Lopez', 'Mendoza', 'Rivera', 'Torres', 'Ramos', 'Aquino', 'Castillo', 'Villanueva', 'Flores', 'Dela Cruz', 'Morales', 'Navarro', 'Pascual'],
    },
}

# Country display names
COUNTRY_NAMES = {
    'US': 'United States',
    'UK': 'United Kingdom',
    'BD': 'Bangladesh',
    'IN': 'India',
    'DE': 'Germany',
    'FR': 'France',
    'BR': 'Brazil',
    'TR': 'Turkey',
    'PK': 'Pakistan',
    'ID': 'Indonesia',
    'PH': 'Philippines',
}


def get_random_name(country: str = 'US') -> tuple[str, str]:
    """Generate a random first and last name for the given country code.
    Returns (first_name, last_name)."""
    country = country.upper().strip()
    if country not in NAMES_BY_COUNTRY:
        country = 'US'  # fallback

    data = NAMES_BY_COUNTRY[country]
    # Randomly pick male or female
    gender = random.choice(['first_male', 'first_female'])
    first = random.choice(data[gender])
    last = random.choice(data['last'])
    return first, last


def get_available_countries() -> list[dict]:
    """Return list of available countries."""
    return [{'code': k, 'name': v} for k, v in COUNTRY_NAMES.items()]
