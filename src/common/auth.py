"""
gestione autenticazione JWT per le API
"""

from datetime import datetime, timedelta, UTC
from jose import JWTError, jwt
from fastapi import HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer

from src.common.config import get_settings
from src.common.models import APITokenPayload
from src.common.utils import utc_now


# scheme per l'autneticazione
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")


def create_access_token(
    client_id: str,
    role: str = "user",
    scopes: list[str] | None = None,
    expires_delta: timedelta | None = None
) -> str:
    """
    creazione jwt di accesso
    
    args:
        client_id: identificativo utente (email)
        role: ruolo utente ('admin' o 'user')
        scopes: lista permessi
        expires_delta: durata token (default da config)
    
    ritorna il token encodato
    """
    settings = get_settings()
    
    if expires_delta is None:
        expires_delta = timedelta(minutes=settings.jwt_expiration_minutes)
    
    now = utc_now()
    expire = now + expires_delta
    
    payload = {
        "sub": client_id,
        "role": role,
        "exp": expire,
        "iat": now,
        "scopes": scopes or []
    }
    
    encoded_jwt = jwt.encode(
        payload,
        settings.jwt_secret,
        algorithm=settings.jwt_algorithm
    )
    
    return encoded_jwt


def decode_token(token: str) -> APITokenPayload:
    """
    decodifica e valida di un JWT token
    """
    settings = get_settings()
    
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm]
        )
        
        exp_timestamp = payload.get("exp")
        iat_timestamp = payload.get("iat")
        
        token_data = APITokenPayload(
            sub=payload.get("sub"),
            role=payload.get("role", "user"),
            exp=datetime.fromtimestamp(exp_timestamp, tz=UTC),
            iat=datetime.fromtimestamp(iat_timestamp, tz=UTC) if iat_timestamp else utc_now(),
            scopes=payload.get("scopes", [])
        )
        
        return token_data
        
    except JWTError as e:
        raise HTTPException(
            status_code=401,
            detail=f"token non valido: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"}
        )


def verify_token(token: str = Depends(oauth2_scheme)) -> APITokenPayload:
    """
    dependency che verifica il token JWT nelle api
    """
    token_data = decode_token(token)
    
    # verifica scadenza
    if token_data.exp < utc_now():
        raise HTTPException(
            status_code=401,
            detail="Token scaduto",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return token_data


def verify_admin(token_data: APITokenPayload = Depends(verify_token)) -> APITokenPayload:
    """
    verificare che l'utente sia admin
    
    args:
        token_data: payload token già verificato
    
    ritorna toekn payload per admin se utente è admin

    """
    if token_data.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="accesso negato: privilegi di amministratore richiesti"
        )
    
    return token_data


def verify_token_with_scopes(required_scopes: list[str]):
    """
    dependency che verifica il token e i suoi scope
    """
    def dependency(token: str = Depends(oauth2_scheme)) -> APITokenPayload:
        token_data = verify_token(token)
        
        token_scopes = set(token_data.scopes)
        required_scopes_set = set(required_scopes)
        
        if not required_scopes_set.issubset(token_scopes):
            missing_scopes = required_scopes_set - token_scopes
            raise HTTPException(
                status_code=403,
                detail=f"permessi insufficienti. scope mancanti: {', '.join(missing_scopes)}"
            )
        
        return token_data
    
    return dependency


def generate_test_token(
    client_id: str = "test-user",
    role: str = "user",
    scopes: list[str] | None = None
) -> str:
    """
    genera un token di test con lunga durata per dev
    """
    return create_access_token(
        client_id=client_id,
        role=role,
        scopes=scopes or ["read", "write"],
        expires_delta=timedelta(hours=24)
    )