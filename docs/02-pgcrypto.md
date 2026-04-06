## `pgcrypto`: hashing og kryptering i databasen

`pgcrypto` er en Postgres-udvidelse der giver kryptografiske funktioner: hashing, tilfældige bytes og PGP-lignende symmetrisk/asymmetrisk kryptering.

### Typiske use cases

- **Password hashing** i DB (typisk `crypt()` + `gen_salt()`).
- **Kryptering af følsomme felter** (CPR, løn, API-nøgler), hvis man *skal* opbevare dem.
- **Signering/hashing** af data for integritet.

### Hashing vs kryptering (vigtigt skel)

- **Hashing**: envejs. Du kan *verificere* men ikke få originalen tilbage.
  - Bruges til passwords.
- **Kryptering**: tovejs. Du kan dekryptere, hvis du har nøglen.
  - Bruges kun når det er et krav, og nøglen skal beskyttes ekstremt godt.

### Passwords: hvorfor `crypt()`?

Når du gemmer et password, gemmer du ikke selve passwordet.

- `crypt(password, gen_salt('bf'))` laver et hash med salt.
- Ved login: `crypt(input, stored_hash)` bruger salt/parametre fra det lagrede hash til at hashe input igen.

Konsekvens:

- **Ingen dekryptering** (hashing er envejs)
- **Salt** gør rainbow tables ubrugelige

### Felt-kryptering: `pgp_sym_encrypt()`

I demoen gemmes følsomme data som `BYTEA` (binary) og krypteres symmetrisk:

- **Krypter**: `pgp_sym_encrypt(plaintext, key)`
- **Dekrypter**: `pgp_sym_decrypt(ciphertext, key)`

Vigtige praksis-pointer til klassen:

- **Nøglen må ikke hardcodes i produktion** (brug KMS/secret manager).
- Overvej om data overhovedet skal ligge i DB (minimering).
- Tænk over adgangskontrol: hvem må dekryptere og hvornår?

### Demo i dette repo

Init-scriptet `docker/initdb/10-pgcrypto-demo.sql` opretter:

- `users` tabel med `password_hash` og `sensitive_data`
- `create_user()` og `verify_user()`

Spørgsmål til elever:

- Hvorfor er `sensitive_data` `BYTEA`?
- Hvorfor giver det mening at “hash” passwords men “kryptere” CPR?
- Hvilke angreb stopper salt? Hvilke stopper det ikke?

