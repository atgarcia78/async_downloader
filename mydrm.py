from pathlib import Path
from threading import Lock
from typing import Callable, Optional

from httpx import Client
from pywidevine.cdm import Cdm
from pywidevine.device import Device, DeviceTypes
from pywidevine.pssh import PSSH

from utils import CLIENT_CONFIG, get_xml

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

    @classmethod
    def create_drm_cdm(cls):
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

        return Cdm.from_device(device)

    @classmethod
    def get_drm_keys(
        cls, lic_url: str, pssh: Optional[str] = None,
        func_validate: Optional[Callable] = None, mpd_url: Optional[str] = None, **kwargs
    ) -> Optional[str]:

        _reduce = lambda x: list(set(map(lambda y: y.text, list(x))))

        if not pssh and mpd_url:
            if (mpd_xml := get_xml(mpd_url, **kwargs)):
                if (_list_pssh := _reduce(
                        mpd_xml.iterfind('.//{urn:mpeg:cenc:2013}pssh'))):
                    pssh = sorted(_list_pssh, key=len)[0]
        if pssh:
            with cls._LOCK:
                if not cls._CDM:
                    cls._CDM = cls.create_drm_cdm()

            session_id = cls._CDM.open()
            challenge = cls._CDM.get_license_challenge(session_id, PSSH(pssh))
            _validate_lic = func_validate or cls.validate_drm_lic
            cls._CDM.parse_license(session_id, _validate_lic(lic_url, challenge))
            if (keys := cls._CDM.get_keys(session_id)):
                for key in keys:
                    if key.type == 'CONTENT':
                        return f"{key.kid.hex}:{key.key.hex()}"

    @classmethod
    def get_drm_xml(
            cls, lic_url: str, file_dest: [str | Path],
            pssh: Optional[str] = None, func_validate: Optional[Callable] = None,
            mpd_url: Optional[str] = None, **kwargs) -> Optional[str]:

        if (_keys := cls.get_drm_keys(
                lic_url, pssh=pssh, func_validate=func_validate,
                mpd_url=mpd_url, **kwargs)):
            with open(file_dest, 'w') as f:
                f.write(CONF_DRM_XML_TEMPLATE % tuple(_keys.split(':')))
            return _keys

    @classmethod
    def validate_drm_lic(self, lic_url: str, challenge: bytes) -> Optional[bytes]:
        with Client(**CLIENT_CONFIG) as client:
            return client.post(lic_url, content=challenge).content
