import logging
import sys
import re
import datetime
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
    decodeArgument,
    shell_quote,
    
    
)

from yt_dlp.postprocessor.ffmpeg import EXT_TO_OUT_FORMATS, FFmpegPostProcessor

from utils import parse_ffmpeg_time_string, compute_prefix, naturalsize


logger = logging.getLogger("async_FFMPEG_DL")


class AsyncFFmpegFD(FFmpegFD):
    
    def _get_cmd(self, args, exe=None):                 

        str_args = [decodeArgument(a) for a in args]

        if exe is None:
            exe = os.path.basename(str_args[0])

        logger.debug(f'{exe} command line: {shell_quote(str_args)}')
        
        return shell_quote(str_args)

    async def _async_call_downloader(self, tmpfilename, info_dict, queue):

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
        args.extend(['-progress', 'pipe:1', '-stats_period', '0.2']) #progress stats to stdout every 0.2secs
        
        try:
        
            if "duration" in info_dict.keys() and info_dict["duration"] is not None and info_dict['duration'] != 0:
                start_time, end_time, total_time_to_dl = None, None, info_dict['duration']
                for i, arg in enumerate(args):
                    if arg == "-ss" and i + 1 < len(args):
                        start_time = parse_ffmpeg_time_string(args[i + 1])
                    elif (arg == "-sseof" or arg == "-t") and i + 1 < len(args):
                        start_time = info_dict['duration'] - parse_ffmpeg_time_string(args[i + 1])
                    elif (arg == "-to" or arg == "-t") and i + 1 < len(args):
                        end_time = parse_ffmpeg_time_string(args[i + 1])
                if start_time is not None and end_time is None:
                    total_time_to_dl = total_time_to_dl - start_time
                elif start_time is None and end_time is not None:
                    total_time_to_dl = end_time
                elif start_time is not None and end_time is not None:
                    total_time_to_dl = end_time - start_time
                started = time.time()
                if "filesize" in info_dict.keys() and info_dict["filesize"]:                    
                    total_filesize = info_dict["filesize"] * total_time_to_dl / info_dict['duration']                    
                elif "filesize_approx" in info_dict.keys() and info_dict["filesize_approx"]:
                    total_filesize = info_dict["filesize_approx"] * total_time_to_dl / info_dict['duration']                    
                else:
                    total_filesize = 0
                   
            else:
                total_filesize = 0
              
            
            status = {
                'filename': tmpfilename,
                'status': 'downloading',
                'total_bytes': total_filesize,
                'filesize_aprox': info_dict.get("filesize_approx"),
                'total_time_to_dl': total_time_to_dl,
                'duration': info_dict.get('duration'),
                'elapsed': time.time() - started,
                'downloaded_bytes': 0
                
            }
            
            logger.debug(f"[{info_dict['id']}][{info_dict['title']}]\n{status}")
            
            queue.put_nowait(status)
            
            cmd = self._get_cmd(args)

            progress_pattern = re.compile(
                    r'(frame=\s*(?P<frame>\S+)\nfps=\s*(?P<fps>\S+)\nstream_0_0_q=\s*(?P<stream_0_0_q>\S+)\n)?bitrate=\s*(?P<bitrate>\S+)\ntotal_size=\s*(?P<total_size>\S+)\nout_time_us=\s*(?P<out_time_us>\S+)\nout_time_ms=\s*(?P<out_time_ms>\S+)\nout_time=\s*(?P<out_time>\S+)\ndup_frames=\s*(?P<dup_frames>\S+)\ndrop_frames=\s*(?P<drop_frames>\S+)\nspeed=\s*(?P<speed>\S+)\nprogress=\s*(?P<progress>\S+)')

            
            proc = await asyncio.create_subprocess_shell(cmd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE) 
            

            async def read_stream(stream):
                
                try:
                    
                    ffpmeg_stdout_buffer = ""
                    
                    while not proc.returncode:
                        try:                        
                            line = await stream.readline()
                        except (asyncio.LimitOverrunError, ValueError):
                            continue
                        status.update({'elapsed': time.time() - started})   
                        if line: 
                            ffmpeg_stdout = line.decode('utf-8')
                            logger.debug(f"[{info_dict['id']}][{info_dict['title']}]: {ffmpeg_stdout}")
                            ffpmeg_stdout_buffer += ffmpeg_stdout
                            ffmpeg_prog_infos = re.match(progress_pattern, ffpmeg_stdout_buffer)
                            if ffmpeg_prog_infos:
                                ffmpeg_stdout = ""
                                speed = 0 if ffmpeg_prog_infos['speed'] == "N/A" else float(ffmpeg_prog_infos['speed'][:-1])
                                if speed != 0:
                                    eta_seconds = (total_time_to_dl - parse_ffmpeg_time_string(
                                        ffmpeg_prog_infos['out_time'])) / speed
                                else:
                                    eta_seconds = 0
                                bitrate_int = None
                                bitrate_str = re.match(r"(?P<E>\d+)(\.(?P<f>\d+))?(?P<U>g|m|k)?bits/s",
                                                    ffmpeg_prog_infos['bitrate'])
                                if bitrate_str:
                                    bitrate_int = compute_prefix(bitrate_str)
                                dl_bytes_str = re.match(r"\d+", ffmpeg_prog_infos['total_size'])
                                dl_bytes_int = int(ffmpeg_prog_infos['total_size']) if dl_bytes_str else 0
                                status.update({
                                    'downloaded_bytes': dl_bytes_int,
                                    'speed': bitrate_int,
                                    'eta': eta_seconds
                                })
                                logger.debug(f"[{info_dict['id']}][{info_dict['title']}]\n{status}")
                                queue.put_nowait(status)
                                ffpmeg_stdout_buffer = ""
                        else:
                            break

                    status.update({
                        'status': 'finished',
                        'downloaded_bytes': total_filesize
                    })
                    logger.debug(f"[{info_dict['id']}][{info_dict['title']}]\n{status}")  
                    queue.put_nowait(status)
                except Exception as e:
                    logger.exception(repr(e))
            
            await asyncio.gather(read_stream(proc.stdout), proc.wait())

        except Exception as e:
            logger.exception(repr(e))

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
    
    
    _EX_FFMPEG = ThreadPoolExecutor(thread_name_prefix="ex_ffmpegdl")
        
    
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
                _filename = self.info_dict.get('filename')            
                self.filename = Path(self.download_path, _filename.stem + "." + self.info_dict['format_id'] + "." + self.info_dict['ext'])

            self.filesize = self.info_dict.get('filesize') or self.info_dict.get('filesize_approx')
            
            self.down_size = 0
            self.dl_cont = {}
            
            self.status = 'init'
            self.error_message = ""
             
            self.reset_event = None
        except Exception as e:
            logger.exception(repr(e))
            raise


    async def update_info_dl(self):
        
        self.downsize_ant = 0
        
        while True:
            status = await self.queue.get()
            async with self.video_downloader.alock:
                if not self.filesize and status.get('total_bytes'):
                    self.filesize = status.get('total_bytes')
                    self.video_downloader.info_dl['filesize'] += self.filesize
                self.down_size = status.get('downloaded_bytes')                
                self.video_downloader.info_dl['down_size'] += (self.down_size - self.downsize_ant)
            
            self.downsize_ant = self.down_size
            
            if status.get('eta', 10000) < 3600:
                _eta = datetime.timedelta(seconds=status.get('eta'))                    
                _eta_str = ":".join([_item.split(".")[0] for _item in f"{_eta}".split(":")[1:]])
            else: _eta_str = "--"
            
            if status.get('speed'):
                _speed_str = f"{naturalsize(status.get('speed'),True)}ps"
            else:
                _speed_str = "--"
            
            _progress_str = f'{(self.down_size/self.filesize)*100:5.2f}%' if self.filesize else '--'
                
            self.dl_cont.update({'speed_str': _speed_str, 'eta_str': _eta_str, 'progress_str': _progress_str})
            
                        
            if status.get('status') == 'finished':
                break
                
            await asyncio.sleep(0)
            
         
    async def fetch_async(self):
        
        self.status = "downloading"
        self.queue = asyncio.Queue()
        try:
           task_dl = asyncio.create_task(self.ffmpegfd._async_call_downloader(str(self.filename), self.info_dict, self.queue))
           task_upt = asyncio.create_task(self.update_info_dl())
           
           done, _ = await asyncio.wait([task_upt, task_dl])
           
        
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
            msg = f"[FFMPEG]: DL[{self.dl_cont.get('speed_str') or '--'}] PR[{self.dl_cont.get('progress_str') or '--'}] ETA[{self.dl_cont.get('eta_str') or '--'}]\n"
            
        return msg
        
               
