from faker import Faker

# Inicijalizacija na engleskom (mo�e i na nema?kom 'de_DE' za tvoje DACH klijente!)
fake = Faker()

print(f"--- AnonifyDB Demo ---")
print(f"Pravi klijent: Ljubomir Colovic")
print(f"Anonimizovano ime: {fake.name()}")
print(f"Lazna adresa za test: {fake.address()}")
print(f"Generisani IBAN za dev nalog: {fake.iban()}")