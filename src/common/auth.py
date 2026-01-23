"""
gestione autenticazione JWT per le API
"""

from datetime import datetime, timedelta, UTC
from jose import JWTError, jwt
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, OAuth2PasswordBearer, OAuth2PasswordRequestForm

from src.common.config import get_settings
from src.common.models import APITokenPayload
from src.common.utils import utc_now


# scheme per l'autneticazione
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/token")

# mantenuto anche HTTPBearer per compatibilità con client esterni, test ecc...
security = HTTPBearer(auto_error=False)


def create_access_token(
    client_id: str,
    scopes: list[str] | None = None,
    expires_delta: timedelta | None = None
) -> str:
    """
    creazione di un JWT token di accesso
    """
    settings = get_settings()
    
    if expires_delta is None:
        expires_delta = timedelta(minutes=settings.jwt_expiration_minutes)
    
    now = utc_now()
    expire = now + expires_delta
    
    payload = {
        "sub": client_id,
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
            detail="token scaduto",
            headers={"WWW-Authenticate": "Bearer"}
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
    scopes: list[str] | None = None
) -> str:
    """
    genera un token di test con lunga durata per dev
    """
    return create_access_token(
        client_id=client_id,
        scopes=scopes or ["read", "write"],
        expires_delta=timedelta(hours=24)
    )