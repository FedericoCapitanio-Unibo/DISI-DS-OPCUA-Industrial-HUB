
# PowerShell -ExecutionPolicy Bypass -File .\setup.ps1

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host " OPC UA Industrial HUB - Setup" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""


function New-JWTSecret {
    $bytes = New-Object byte[] 32
    [System.Security.Cryptography.RNGCryptoServiceProvider]::Create().GetBytes($bytes)
    return [Convert]::ToBase64String($bytes)
}

function Test-Email {
    param([string]$Email)
    $emailRegex = '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return $Email -match $emailRegex
}

function Test-PasswordComplexity {
    param([string]$Password)
    
    $hasMinLength = $Password.Length -ge 8
    $hasUpper = $Password -cmatch '[A-Z]'
    $hasLower = $Password -cmatch '[a-z]'
    $hasDigit = $Password -cmatch '[0-9]'
    $hasSpecial = $Password -match '[^a-zA-Z0-9]'
    
    return $hasMinLength -and $hasUpper -and $hasLower -and $hasDigit -and $hasSpecial
}


Write-Host "Verifica certificati TLS..." -ForegroundColor Yellow

$certsDir = "nginx/certs"
$certFile = "$certsDir/server.crt"
$keyFile = "$certsDir/server.key"

$needsGeneration = $false
$certExists = (Test-Path $certFile) -and (Test-Path $keyFile)

if (-not $certExists) {
    Write-Host "Certificati non trovati" -ForegroundColor Gray
    $needsGeneration = $true
} else {
    #check scadenza certificato
    try {
        $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($certFile)
        $daysUntilExpiry = ($cert.NotAfter - (Get-Date)).Days
        
        if ($daysUntilExpiry -lt 30) {
            Write-Host "Certificati in scadenza tra $daysUntilExpiry giorni" -ForegroundColor Yellow
            $needsGeneration = $true
        } else {
            Write-Host "Certificati validi (scadenza tra $daysUntilExpiry giorni)" -ForegroundColor Gray
            $regenerate = Read-Host "Vuoi rigenerarli? (s/N)"
            if ($regenerate -eq "s" -or $regenerate -eq "S") {
                $needsGeneration = $true
            }
        }
    } catch {
        Write-Host "Errore lettura certificato esistente" -ForegroundColor Yellow
        $needsGeneration = $true
    }
}

if ($needsGeneration) {
    Write-Host "Generazione certificati TLS..." -ForegroundColor Yellow
    
    # creo la dir se non c'è
    if (-not (Test-Path $certsDir)) {
        New-Item -ItemType Directory -Path $certsDir -Force | Out-Null
    }
    
    # generazione certs tls
    try {
        $dockerCmd = "docker run --rm -v `"${PWD}/nginx/certs:/certs`" alpine/openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout /certs/server.key -out /certs/server.crt -subj `"/C=IT/ST=Italy/L=Bologna/O=DISI/CN=localhost`""
        
        Invoke-Expression $dockerCmd | Out-Null
        
        if ((Test-Path $certFile) -and (Test-Path $keyFile)) {
            Write-Host "OK (certificati generati)" -ForegroundColor Green
        } else {
            Write-Host "ERRORE: Certificati non generati correttamente" -ForegroundColor Red
        }
    } catch {
        Write-Host "ERRORE: Impossibile generare certificati" -ForegroundColor Red
        Write-Host "Verifica che Docker sia in esecuzione" -ForegroundColor Gray
    }
} else {
    Write-Host "OK (certificati esistenti)" -ForegroundColor Green
}

Write-Host ""


# check esistenza file

if (-not (Test-Path ".env.example")) {
    Write-Host "ERRORE: File .env.example non trovato!" -ForegroundColor Red
    Write-Host "Assicurati di essere nella root del progetto." -ForegroundColor Gray
    exit 1
}

if (Test-Path ".env") {
    Write-Host "Il file .env esiste gia!" -ForegroundColor Yellow
    $overwrite = Read-Host "Vuoi sovrascriverlo? (s/N)"
    if ($overwrite -ne "s" -and $overwrite -ne "S") {
        Write-Host "Setup annullato." -ForegroundColor Gray
        exit 0
    }
    Write-Host ""
}


# setuppare le credenziali di admin

Write-Host "[1/3] Configurazione Admin Email" -ForegroundColor Yellow
Write-Host ""
Write-Host "Formato richiesto: user@domain.com" -ForegroundColor Gray
Write-Host ""

$adminEmail = ""
$validEmail = $false

while (-not $validEmail) {
    $adminEmail = Read-Host "Admin email"
    $adminEmail = $adminEmail.Trim()
    
    if ([string]::IsNullOrWhiteSpace($adminEmail)) {
        Write-Host "Email non puo essere vuota!" -ForegroundColor Red
        continue
    }
    
    if (-not (Test-Email -Email $adminEmail)) {
        Write-Host "Email non valida!" -ForegroundColor Red
        continue
    }
    
    $validEmail = $true
    Write-Host "OK" -ForegroundColor Green
}

Write-Host ""


Write-Host "[2/3] Configurazione Admin Password" -ForegroundColor Yellow
Write-Host ""
Write-Host "Requisiti: min 8 caratteri, maiuscola, minuscola, numero, carattere speciale" -ForegroundColor Gray
Write-Host ""

$adminPassword = ""
$validPassword = $false

while (-not $validPassword) {
    $securePassword = Read-Host "Admin password" -AsSecureString
    $password = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePassword)
    )
    
    $password = $password.Trim()
    
    if ([string]::IsNullOrWhiteSpace($password)) {
        Write-Host "Password non puo essere vuota!" -ForegroundColor Red
        continue
    }
    
    if (-not (Test-PasswordComplexity -Password $password)) {
        Write-Host "Password non soddisfa i requisiti!" -ForegroundColor Red
        continue
    }
    
    $securePasswordConfirm = Read-Host "Conferma password" -AsSecureString
    $passwordConfirm = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
        [Runtime.InteropServices.Marshal]::SecureStringToBSTR($securePasswordConfirm)
    )
    
    $passwordConfirm = $passwordConfirm.Trim()
    
    if ($password -ne $passwordConfirm) {
        Write-Host "Le password non coincidono!" -ForegroundColor Red
        continue
    }
    
    $validPassword = $true
    $adminPassword = $password
    Write-Host "OK" -ForegroundColor Green
}

Write-Host ""


# generazione jwt secret

Write-Host "[3/3] Generazione JWT Secret Key" -ForegroundColor Yellow
$jwtSecret = New-JWTSecret
Write-Host "OK" -ForegroundColor Green
Write-Host ""


Write-Host "[4/4] Configurazione Utenti (opzionale)" -ForegroundColor Yellow
Write-Host ""
Write-Host "Vuoi aggiungere utenti con accesso limitato? (s/N)" -ForegroundColor Gray
$addUsers = Read-Host

$usersList = @()

if ($addUsers -eq "s" -or $addUsers -eq "S") {
    $addMore = $true
    
    while ($addMore) {
        Write-Host ""
        Write-Host "--- Nuovo utente ---" -ForegroundColor Cyan
        
        $userEmail = ""
        $validUserEmail = $false
        
        while (-not $validUserEmail) {
            $userEmail = Read-Host "Email utente"
            $userEmail = $userEmail.Trim()
            
            if ([string]::IsNullOrWhiteSpace($userEmail)) {
                Write-Host "Email non puo essere vuota!" -ForegroundColor Red
                continue
            }
            
            if (-not (Test-Email -Email $userEmail)) {
                Write-Host "Email non valida!" -ForegroundColor Red
                continue
            }
            
            # check email già usata
            if ($userEmail -eq $adminEmail) {
                Write-Host "Email gia usata dall'admin!" -ForegroundColor Red
                continue
            }
            
            $emailExists = $usersList | Where-Object { $_.StartsWith("${userEmail}:") }
            if ($emailExists) {
                Write-Host "Email gia usata da un altro utente!" -ForegroundColor Red
                continue
            }
            
            $validUserEmail = $true
            Write-Host "OK" -ForegroundColor Green
        }
        
        $userPassword = ""
        $validUserPassword = $false
        
        while (-not $validUserPassword) {
            $secureUserPassword = Read-Host "Password utente" -AsSecureString
            $userPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureUserPassword)
            )
            
            $userPassword = $userPassword.Trim()
            
            if ([string]::IsNullOrWhiteSpace($userPassword)) {
                Write-Host "Password non puo essere vuota!" -ForegroundColor Red
                continue
            }
            
            if (-not (Test-PasswordComplexity -Password $userPassword)) {
                Write-Host "Password non soddisfa i requisiti!" -ForegroundColor Red
                Write-Host "Requisiti: min 8 caratteri, maiuscola, minuscola, numero, carattere speciale" -ForegroundColor Gray
                continue
            }
            
            $secureUserPasswordConfirm = Read-Host "Conferma password" -AsSecureString
            $userPasswordConfirm = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
                [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureUserPasswordConfirm)
            )
            
            $userPasswordConfirm = $userPasswordConfirm.Trim()
            
            if ($userPassword -ne $userPasswordConfirm) {
                Write-Host "Le password non coincidono!" -ForegroundColor Red
                continue
            }
            
            $validUserPassword = $true
            Write-Host "OK" -ForegroundColor Green
        }
        
        $usersList += "${userEmail}:${userPassword}"
        Write-Host "Utente $userEmail aggiunto con successo" -ForegroundColor Green
        
        Write-Host ""
        $continue = Read-Host "Aggiungere un altro utente? (s/N)"
        if ($continue -ne "s" -and $continue -ne "S") {
            $addMore = $false
        }
    }
    
    Write-Host ""
    Write-Host "Totale utenti aggiunti: $($usersList.Count)" -ForegroundColor Green
}

$usersString = ""
if ($usersList.Count -gt 0) {
    $usersString = $usersList -join ","
}

Write-Host ""


# creazione file .env

Write-Host "Creazione file .env..." -ForegroundColor Yellow

$envContent = "JWT_SECRET=$jwtSecret`n"
$envContent += "ADMIN_USERNAME=$adminEmail`n"
$envContent += "ADMIN_PASSWORD=$adminPassword`n"
$envContent += "USERS=$usersString`n"

[System.IO.File]::WriteAllText("$PWD\.env", $envContent, [System.Text.UTF8Encoding]::new($false))

Write-Host "OK" -ForegroundColor Green
Write-Host ""



Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host " Setup Completato!" -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "File .env creato con:"
Write-Host "  - JWT_SECRET (generato)" -ForegroundColor Gray
Write-Host "  - ADMIN_USERNAME: $adminEmail" -ForegroundColor Gray
Write-Host "  - ADMIN_PASSWORD: ****" -ForegroundColor Gray
if ($usersList.Count -gt 0) {
    Write-Host "  - USERS: $($usersList.Count) utenti configurati" -ForegroundColor Gray
    foreach ($user in $usersList) {
        $userEmail = $user.Split(':')[0]
        Write-Host "    - $userEmail" -ForegroundColor DarkGray
    }
}
Write-Host ""
Write-Host "Prossimo passo: docker-compose up -d" -ForegroundColor Gray
Write-Host ""