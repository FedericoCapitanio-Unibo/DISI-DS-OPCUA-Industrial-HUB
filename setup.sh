#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
GRAY='\033[0;90m'
NC='\033[0m'

echo ""
echo -e "${CYAN}=====================================================${NC}"
echo -e "${CYAN} OPC UA Industrial HUB - Setup${NC}"
echo -e "${CYAN}=====================================================${NC}"

echo ""



generate_jwt_secret() {
    openssl rand -base64 32
}

validate_email() {
    local email="$1"
    local regex='^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    [[ $email =~ $regex ]]
}

validate_password() {
    local password="$1"
    
    [ ${#password} -ge 8 ] && \
    echo "$password" | grep -q '[A-Z]' && \
    echo "$password" | grep -q '[a-z]' && \
    echo "$password" | grep -q '[0-9]' && \
    echo "$password" | grep -q '[^a-zA-Z0-9]'
}

read_secret() {
    local prompt="$1"
    local secret
    stty -echo
    read -p "$prompt" secret
    stty echo
    echo ""
    echo "$secret"
}



echo -e "${YELLOW}Verifica certificati TLS...${NC}"

certs_dir="nginx/certs"
cert_file="$certs_dir/server.crt"
key_file="$certs_dir/server.key"

needs_generation=false

if [ ! -f "$cert_file" ] || [ ! -f "$key_file" ]; then
    echo -e "${GRAY}Certificati non trovati${NC}"
    needs_generation=true
else
    # check scadenza certificato
    expiry_date=$(openssl x509 -enddate -noout -in "$cert_file" 2>/dev/null | cut -d= -f2)
    
    if [ -n "$expiry_date" ]; then
        expiry_epoch=$(date -d "$expiry_date" +%s 2>/dev/null || date -j -f "%b %d %H:%M:%S %Y %Z" "$expiry_date" +%s 2>/dev/null)
        current_epoch=$(date +%s)
        days_until_expiry=$(( ($expiry_epoch - $current_epoch) / 86400 ))
        
        if [ $days_until_expiry -lt 30 ]; then
            echo -e "${YELLOW}Certificati in scadenza tra $days_until_expiry giorni${NC}"
            needs_generation=true
        else
            echo -e "${GRAY}Certificati validi (scadenza tra $days_until_expiry giorni)${NC}"
            read -p "Vuoi rigenerarli? (s/N): " regenerate
            if [ "$regenerate" = "s" ] || [ "$regenerate" = "S" ]; then
                needs_generation=true
            fi
        fi
    else
        echo -e "${YELLOW}Errore lettura certificato esistente${NC}"
        needs_generation=true
    fi
fi

if [ "$needs_generation" = true ]; then
    echo -e "${YELLOW}Generazione certificati TLS...${NC}"
    
    # creazione dir se non c'è già
    mkdir -p "$certs_dir"
    
    # creazione certificati
    if docker run --rm -v "${PWD}/nginx/certs:/certs" alpine/openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /certs/server.key -out /certs/server.crt -subj "/C=IT/ST=Italy/L=Bologna/O=DISI/CN=localhost" > /dev/null 2>&1; then
        if [ -f "$cert_file" ] && [ -f "$key_file" ]; then
            echo -e "${GREEN}OK (certificati generati)${NC}"
        else
            echo -e "${RED}ERRORE: Certificati non generati correttamente${NC}"
        fi
    else
        echo -e "${RED}ERRORE: Impossibile generare certificati${NC}"
        echo -e "${GRAY}Verifica che Docker sia in esecuzione${NC}"
    fi
else
    echo -e "${GREEN}OK (certificati esistenti)${NC}"
fi

echo ""




if ! command -v openssl &> /dev/null; then
    echo -e "${RED}ERRORE: openssl non trovato!${NC}"
    exit 1
fi

if [ ! -f ".env.example" ]; then
    echo -e "${RED}ERRORE: File .env.example non trovato!${NC}"
    exit 1
fi


# check esistenza file
if [ -f ".env" ]; then
    echo -e "${YELLOW}Il file .env esiste gia!${NC}"
    read -p "Vuoi sovrascriverlo? (s/N): " overwrite
    if [ "$overwrite" != "s" ] && [ "$overwrite" != "S" ]; then
        echo "Setup annullato."
        exit 0
    fi
    echo ""
fi

# setuppare le credenziali di admin

echo -e "${YELLOW}[1/3] Configurazione Admin Email${NC}"
echo ""
echo -e "${GRAY}Formato richiesto: user@domain.com${NC}"
echo ""

admin_email=""
valid_email=false

while [ "$valid_email" = false ]; do
    read -p "Admin email: " admin_email
    admin_email=$(echo "$admin_email" | xargs)
    
    if [ -z "$admin_email" ]; then
        echo -e "${RED}Email non puo essere vuota!${NC}"
        continue
    fi
    
    if ! validate_email "$admin_email"; then
        echo -e "${RED}Email non valida!${NC}"
        continue
    fi
    
    valid_email=true
    echo -e "${GREEN}OK${NC}"
done

echo ""


echo -e "${YELLOW}[2/3] Configurazione Admin Password${NC}"
echo ""
echo -e "${GRAY}Requisiti: min 8 caratteri, maiuscola, minuscola, numero, carattere speciale${NC}"
echo ""

admin_password=""
valid_password=false

while [ "$valid_password" = false ]; do
    password=$(read_secret "Admin password: ")
    password=$(echo "$password" | xargs)
    
    if [ -z "$password" ]; then
        echo -e "${RED}Password non puo essere vuota!${NC}"
        continue
    fi
    
    if ! validate_password "$password"; then
        echo -e "${RED}Password non soddisfa i requisiti!${NC}"
        continue
    fi
    
    echo ""
    password_confirm=$(read_secret "Conferma password: ")
    password_confirm=$(echo "$password_confirm" | xargs)
    
    if [ "$password" != "$password_confirm" ]; then
        echo -e "${RED}Le password non coincidono!${NC}"
        continue
    fi
    
    valid_password=true
    admin_password="$password"
    echo -e "${GREEN}OK${NC}"
done

echo ""

#generazione jwt secret
echo -e "${YELLOW}[3/3] Generazione JWT Secret Key${NC}"
jwt_secret=$(generate_jwt_secret)
echo -e "${GREEN}OK${NC}"
echo ""

# creazione file .env

echo -e "${YELLOW}Creazione file .env...${NC}"

cat > .env << EOF
JWT_SECRET=$jwt_secret
ADMIN_USERNAME=$admin_email
ADMIN_PASSWORD=$admin_password
EOF

echo -e "${GREEN}OK${NC}"
echo ""


chmod 600 .env



echo -e "${CYAN}=====================================================${NC}"
echo -e "${GREEN} Setup Completato!${NC}"
echo -e "${CYAN}=====================================================${NC}"
echo ""
echo "File .env creato con:"
echo -e "${GRAY}  - JWT_SECRET (generato)${NC}"
echo -e "${GRAY}  - ADMIN_USERNAME: $admin_email${NC}"
echo -e "${GRAY}  - ADMIN_PASSWORD: ****${NC}"
echo ""
echo -e "${GRAY}Prossimo passo: docker-compose up -d${NC}"
echo ""