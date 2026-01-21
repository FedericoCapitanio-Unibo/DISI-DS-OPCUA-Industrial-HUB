"""
gestione autenticazione JWT per le API
"""

from datetime import datetime, timedelta, UTC
from jose import JWTError, jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.common.config import get_settings
from src.common.models import APITokenPayload
from src.common.utils import utc_now


# security scheme della lib fastapi
security = HTTPBearer()


def create_access_token(
    client_id: str,
    scopes: list[str] | None = None,
    expires_delta: timedelta | None = None
) -> str:
    """
    creazione di un JWT token di accesso
    
    args:
        client_id: identificativo dell'id del client
        scopes: lista di permessi da includere nel token
        expires_delta: durata del token, se è None usa default da config
    
    returns:
        token JWT codificato come stringa

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
    
    args:
        token: token JWT da decodificare
    
    returns:
        payload del token validato

    
    raisa eccezione se il token non è valido

    """
    settings = get_settings()
    
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm]
        )
        
        # conversione datetime da timestamp unix
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


def verify_token(
    credentials: HTTPAuthorizationCredentials = Security(security)

) -> APITokenPayload:
    """
    dependency che verifica il token JWT nelle api
    
    args:
        credentials: credenziali HTTP Bearer estratte dall'header Authorization
    
    returns:
        payload del token validato
    
    propaga eccezione 401 se manca il token, se è invalido o se è scaduto
    """
    token = credentials.credentials
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
    
    args:
        required_scopes: lista di scope richiesti per accedere alla risorsa

    """
    def dependency(
        credentials: HTTPAuthorizationCredentials = Security(security)
    ) -> APITokenPayload:
        token_data = verify_token(credentials)
        
        #verifica che tutti gli scope richiesti siano presenti
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
    
    args:
        client_id: identificativo dell'utente
        scopes: scope da includere
    
    
    si ottiene un token JWT valido per 24 oree
    """
    return create_access_token(
        client_id=client_id,
        scopes=scopes or ["read", "write"],
        expires_delta=timedelta(hours=24)
    )