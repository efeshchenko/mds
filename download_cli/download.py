# -*- coding: utf-8 -*-

import os
import json
import urllib
import time
import sys
import codecs

# TODO: 1. Cancel download it it's not progressing
# TODO: 3. Store all known links
# TODO: 4. Make scrappy store data to sqlite


def reporthook(count, block_size, total_size):
    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    if percent < 0:
        percent = '? '
    sys.stdout.write("\r... %d%%, %d MB, %d KB/s, %d секунд" % (percent, progress_size / (1024 * 1024), speed, duration))
    sys.stdout.flush()


def download():

    with open('input/data.json', 'r') as f:
        data = f.read()
        data = json.loads(data)

    data.sort(key=lambda x: x['number'])

    counter = 0
    line = 0
    while counter < 30:
        if not data[line]['downloaded']:
            filename = u'{}.{} - {}.mp3'.format(data[line]['number'], data[line]['author'], data[line]['name'])
            print(filename)
            try:
                urllib.urlretrieve(data[line]['link'], 'output/'+filename, reporthook)
                data[line]['downloaded'] = True
            except Exception as e:
                data[line]['downloaded'] = 'err'
                print('    Ошибка скачивания.')
            finally:
                with codecs.open('input/data.json', 'w+', encoding='utf-8') as f:
                    f.write(json.dumps(data, ensure_ascii=False, indent=2))
                counter += 1
        line += 1  # go to next line
    else:
        print('')
        print('Готово!')

    for f in os.listdir('output'):
        if os.path.getsize('output/' + f) < 1000000:
            os.remove('output/' + f)

    sorted([os.path.getsize('output/' + x) for x in os.listdir('output')])


if __name__ == '__main__':
    download()
