from __future__ import annotations

import functools
import logging
import os
from pathlib import Path
from threading import Lock, Semaphore
from typing import Callable, Optional

from httpx import Client
from pywidevine.cdm import Cdm
from pywidevine.device import Device, DeviceTypes
from pywidevine.pssh import PSSH

from utils import CLIENT_CONFIG, get_pssh_from_manifest, try_call, variadic

logger = logging.getLogger('mydrm')

CONF_DRM_BASE_PATH = Path(
    Path.home(),
    'Projects/dumper/key_dumps/Android Emulator 5554/private_keys/7283/2049378471')

CONF_DRM = {
    "private_key": Path(CONF_DRM_BASE_PATH, 'private_key.pem'),
    "client_id": Path(CONF_DRM_BASE_PATH, 'client_id.bin')
}

CONF_DRM_XML_TEMPLATE = '''\
<?xml version="1.0" encoding="UTF-8"?>
<GPACDRM type="CENC AES-CTR">
<CrypTrack IV_size="16" first_IV="0xedef8ba979d64acea3c827dcd51d21ed">
<key KID="0x%s" value="0x%s"/>
</CrypTrack>
</GPACDRM>'''


class myDRM:
    _LOCK = Lock()
    _CDM = None
    _SEM = Semaphore(12)

    class syncsem:
        def __call__(self, func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                with myDRM._SEM:
                    return func(*args, **kwargs)
            return wrapper

    @classmethod
    def create_drm_cdm(
        cls,
        file: Optional[str | Path] = None
    ) -> Cdm:
        if file and os.path.exists(file):
            device = Device.load(file)
        else:
            with open(CONF_DRM['private_key']) as fpriv:
                _private_key = fpriv.read()
            with open(CONF_DRM['client_id'], "rb") as fpid:
                _client_id = fpid.read()
            device = Device(
                type_=DeviceTypes.ANDROID,
                security_level=3,
                flags={},
                client_id=_client_id,
                private_key=_private_key)

        cls.close_sessions()
        cls._CDM = Cdm.from_device(device)
        return cls._CDM

    @classmethod
    @syncsem()
    def get_drm_keys(
        cls,
        lic_url: str,
        pssh: Optional[str] = None,
        func_validate: Optional[Callable] = None,
        manifest_url: Optional[str] = None,
        manifest_doc: Optional[str] = None,
        **kwargs
    ) -> Optional[str | list]:

        if not pssh:
            pssh = try_call(lambda: get_pssh_from_manifest(manifest_url=manifest_url, **kwargs)[0])
            if not pssh:
                raise ValueError('couldnt find pssh')

        _validate_lic = func_validate or cls.validate_drm_lic
        with cls._LOCK:
            if not cls._CDM:
                cls._CDM = cls.create_drm_cdm()

        session_id = cls._CDM.open()
        try:
            challenge = cls._CDM.get_license_challenge(session_id, PSSH(pssh))
            cls._CDM.parse_license(session_id, _validate_lic(lic_url, challenge, **kwargs))
            if (keys := cls._CDM.get_keys(session_id)):
                if _res := [
                    f"{key.kid.hex}:{key.key.hex()}"
                    for key in keys
                    if key.type == 'CONTENT'
                ]:
                    return _res if len(_res) > 1 else _res[0]
        finally:
            cls._CDM.close(session_id)

    @classmethod
    def get_drm_xml(
        cls,
        lic_url: str,
        file_dest: str | Path,
        pssh: Optional[str] = None,
        func_validate: Optional[Callable] = None,
        manifest_url: Optional[str] = None,
        manifest_doc: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:

        if (
            _keys := variadic(cls.get_drm_keys(
                lic_url, pssh=pssh, func_validate=func_validate,
                manifest_url=manifest_url, **kwargs))
        ):

            cls._write_xml(file_dest, _keys[0])
            return _keys[0]

    @classmethod
    def _write_xml(cls, file_dest, key):
        with open(file_dest, 'w') as f:
            f.write(CONF_DRM_XML_TEMPLATE % tuple(key.split(':')))

    @classmethod
    def validate_drm_lic(
        cls,
        lic_url: str,
        challenge: bytes, **kwargs
    ) -> Optional[bytes]:
        with Client(**CLIENT_CONFIG) as client:
            resp = client.post(lic_url, content=challenge, **kwargs)
            logger.debug(f"[validate_lic] {resp}, {resp.request}, {resp.request.headers}")
            return resp.content

    @classmethod
    def close_sessions(cls):
        if cls._CDM:
            for sessid, _ in cls._CDM._Cdm__sessions.items():
                myDRM._CDM.close(sessid)
