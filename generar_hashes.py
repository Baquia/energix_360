from flask_bcrypt import Bcrypt

bcrypt = Bcrypt()

passwords = [
    "Metanoia765/*",  
]

for pwd in passwords:
    hashed = bcrypt.generate_password_hash(pwd).decode('utf-8')
    print(f"{pwd} → {hashed}")


