import copy
import logging
import sys
import re
import subprocess
import os
import asyncio
import time

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from yt_dlp.downloader import FFmpegFD

from yt_dlp.utils import (
    determine_ext,
    encodeArgument,
    encodeFilename,
    handle_youtubedl_headers,
    remove_end,
    traverse_obj,
)

from yt_dlp.postprocessor.ffmpeg import EXT_TO_OUT_FORMATS, FFmpegPostProcessor

from utils import async_ex_in_executor, none_to_cero

from threading import Lock


logger = logging.getLogger("async_FFMPEG_DL")



class AsyncFFmpegFD(FFmpegFD):
    
    
    async def _postffmpeg(self, cmd, _id, _title):        
        
        
        logger.debug(f"[{_id}][{_title}]:{cmd}")
        
        proc = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=1024 * 1024) 
        
        async def read_stream(stream):
            msg = ""
            while (proc is not None and not proc.returncode):
                try:
                    line = await stream.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    continue
                if line: 
                    line = line.decode('utf-8').strip()
                    msg = f"{msg}{line}\n"                                                            
                else:
                    break
            logger.debug(f"[{_id}][{_title}]:{msg}")
            
        await asyncio.gather(read_stream(proc.stdout), read_stream(proc.stderr), proc.wait())
        
        return proc.returncode
    

   
    def _call_downloader(self, tmpfilename, info_dict):

        urls = [f['url'] for f in info_dict.get('requested_formats', [])] or [info_dict['url']]
        ffpp = FFmpegPostProcessor(downloader=self)
        if not ffpp.available:
            self.report_error('m3u8 download detected but ffmpeg could not be found. Please install')
            return False
        ffpp.check_version()

        args = [ffpp.executable, '-y']

        for log_level in ('quiet', 'verbose'):
            if self.params.get(log_level, False):
                args += ['-loglevel', log_level]
                break
        if not self.params.get('verbose'):
            args += ['-hide_banner']

        args += traverse_obj(info_dict, ('downloader_options', 'ffmpeg_args'), default=[])

        # These exists only for compatibility. Extractors should use
        # info_dict['downloader_options']['ffmpeg_args'] instead
        args += info_dict.get('_ffmpeg_args') or []
        seekable = info_dict.get('_seekable')
        if seekable is not None:
            # setting -seekable prevents ffmpeg from guessing if the server
            # supports seeking(by adding the header `Range: bytes=0-`), which
            # can cause problems in some cases
            # https://github.com/ytdl-org/youtube-dl/issues/11800#issuecomment-275037127
            # http://trac.ffmpeg.org/ticket/6125#comment:10
            args += ['-seekable', '1' if seekable else '0']

        http_headers = None
        if info_dict.get('http_headers'):
            youtubedl_headers = handle_youtubedl_headers(info_dict['http_headers'])
            http_headers = [
                # Trailing \r\n after each HTTP header is important to prevent warning from ffmpeg/avconv:
                # [http @ 00000000003d2fa0] No trailing CRLF found in HTTP header.
                '-headers',
                ''.join(f'{key}: {val}\r\n' for key, val in youtubedl_headers.items())
            ]

        env = None
        proxy = self.params.get('proxy')
        if proxy:
            if not re.match(r'^[\da-zA-Z]+://', proxy):
                proxy = 'http://%s' % proxy

            if proxy.startswith('socks'):
                self.report_warning(
                    '%s does not support SOCKS proxies. Downloading is likely to fail. '
                    'Consider adding --hls-prefer-native to your command.' % self.get_basename())

            # Since December 2015 ffmpeg supports -http_proxy option (see
            # http://git.videolan.org/?p=ffmpeg.git;a=commit;h=b4eb1f29ebddd60c41a2eb39f5af701e38e0d3fd)
            # We could switch to the following code if we are able to detect version properly
            # args += ['-http_proxy', proxy]
            env = os.environ.copy()
            env['HTTP_PROXY'] = proxy
            env['http_proxy'] = proxy

        protocol = info_dict.get('protocol')

        if protocol == 'rtmp':
            player_url = info_dict.get('player_url')
            page_url = info_dict.get('page_url')
            app = info_dict.get('app')
            play_path = info_dict.get('play_path')
            tc_url = info_dict.get('tc_url')
            flash_version = info_dict.get('flash_version')
            live = info_dict.get('rtmp_live', False)
            conn = info_dict.get('rtmp_conn')
            if player_url is not None:
                args += ['-rtmp_swfverify', player_url]
            if page_url is not None:
                args += ['-rtmp_pageurl', page_url]
            if app is not None:
                args += ['-rtmp_app', app]
            if play_path is not None:
                args += ['-rtmp_playpath', play_path]
            if tc_url is not None:
                args += ['-rtmp_tcurl', tc_url]
            if flash_version is not None:
                args += ['-rtmp_flashver', flash_version]
            if live:
                args += ['-rtmp_live', 'live']
            if isinstance(conn, list):
                for entry in conn:
                    args += ['-rtmp_conn', entry]
            elif isinstance(conn, str):
                args += ['-rtmp_conn', conn]

        start_time, end_time = info_dict.get('section_start') or 0, info_dict.get('section_end')

        for i, url in enumerate(urls):
            if http_headers is not None and re.match(r'^https?://', url):
                args += http_headers
            if start_time:
                args += ['-ss', str(start_time)]
            if end_time:
                args += ['-t', str(end_time - start_time)]

            args += self._configuration_args((f'_i{i + 1}', '_i')) + ['-i', url]

        if not (start_time or end_time) or not self.params.get('force_keyframes_at_cuts'):
            args += ['-c', 'copy']

        if info_dict.get('requested_formats') or protocol == 'http_dash_segments':
            for (i, fmt) in enumerate(info_dict.get('requested_formats') or [info_dict]):
                stream_number = fmt.get('manifest_stream_number', 0)
                args.extend(['-map', f'{i}:{stream_number}'])

        if self.params.get('test', False):
            args += ['-fs', str(self._TEST_FILE_SIZE)]

        ext = info_dict['ext']
        if protocol in ('m3u8', 'm3u8_native'):
            use_mpegts = (tmpfilename == '-') or self.params.get('hls_use_mpegts')
            if use_mpegts is None:
                use_mpegts = info_dict.get('is_live')
            if use_mpegts:
                args += ['-f', 'mpegts']
            else:
                args += ['-f', 'mp4']
                if (ffpp.basename == 'ffmpeg' and ffpp._features.get('needs_adtstoasc')) and (not info_dict.get('acodec') or info_dict['acodec'].split('.')[0] in ('aac', 'mp4a')):
                    args += ['-bsf:a', 'aac_adtstoasc']
        elif protocol == 'rtmp':
            args += ['-f', 'flv']
        elif ext == 'mp4' and tmpfilename == '-':
            args += ['-f', 'mpegts']
        elif ext == 'unknown_video':
            ext = determine_ext(remove_end(tmpfilename, '.part'))
            if ext == 'unknown_video':
                self.report_warning(
                    'The video format is unknown and cannot be downloaded by ffmpeg. '
                    'Explicitly set the extension in the filename to attempt download in that format')
            else:
                self.report_warning(f'The video format is unknown. Trying to download as {ext} according to the filename')
                args += ['-f', EXT_TO_OUT_FORMATS.get(ext, ext)]
        else:
            args += ['-f', EXT_TO_OUT_FORMATS.get(ext, ext)]

        args += self._configuration_args(('_o1', '_o', ''))

        args = [encodeArgument(opt) for opt in args]
        args.append(encodeFilename(ffpp._ffmpeg_filename_argument(tmpfilename), True))
        self._debug_cmd(args)

        proc = subprocess.run(args, encoding='utf-8', capture_output=True)
        
        logger.debug(f"[{info_dict['id']}][{info_dict['title']}] {proc.stderr}")

        return proc.returncode



class AsyncFFMPEGDLErrorFatal(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncFFMPEGDLErrorFatal, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncFFMPEGDLError(Exception):
    """Error during info extraction."""

    def __init__(self, msg):
        
        super(AsyncFFMPEGDLError, self).__init__(msg)

        self.exc_info = sys.exc_info()  # preserve original exception

class AsyncFFMPEGDownloader():
    
  
    _LOCK = Lock()    
    _EX_FFMPEG = None
        
    
    def __init__(self, video_dict, vid_dl):
        
        try:

            self.info_dict = video_dict.copy()
            self.video_downloader = vid_dl                
            
                
            self.ytdl = self.video_downloader.info_dl['ytdl']              
        
            self.ffmpegfd = AsyncFFmpegFD(self.ytdl, {})
                        
                        
            self.download_path = self.info_dict['download_path']
            
            self.download_path.mkdir(parents=True, exist_ok=True) 
            if (_filename:=self.info_dict.get('_filename')):            
                
                self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])
            else:
                # self.download_path = self.base_download_path
                _filename = self.info_dict.get('filename')            
                self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])

            self.filesize = none_to_cero((self.info_dict.get('filesize', 0)))
            
            self.down_size = 0
            
            self.status = 'init'
            self.error_message = ""
            
                    
            with AsyncFFMPEGDownloader._LOCK:
                if not AsyncFFMPEGDownloader._EX_FFMPEG:
                    AsyncFFMPEGDownloader._EX_FFMPEG = ThreadPoolExecutor(thread_name_prefix="ex_aria2dl")
            
            self.reset_event = None
        except Exception as e:
            logger.exception(repr(e))
            raise



    async def fetch_async(self):
        
        self.status = "downloading"
        try:
            await async_ex_in_executor(AsyncFFMPEGDownloader._EX_FFMPEG, self.ffmpegfd.real_download, str(self.filename), self.info_dict)
           
        
        except Exception as e:
            logger.error(f"[{self.info_dict['id']}][{self.info_dict['title']}] error {repr(e)}")
            self.status = "error"
        finally:
                        
            logger.debug(f"[{self.info_dict['id']}][{self.info_dict['title']}]: DL completed")
            self.status = "done"
                

    def print_hookup(self):
        
        msg = ""
        
        if self.status == "done":
            msg = f"[FFMPEG]: Completed\n"
        elif self.status == "init":
            msg = f"[FFMPEG]: Waiting to DL\n"       
        elif self.status == "error":
            msg = f"[FFMPEG]: ERROR\n"        
        elif self.status == "downloading":        
            msg = f"[FFMPEG]: Downloading\n"
            
        return msg
        
               
