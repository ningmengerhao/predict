import cv2
import os
import numpy as np
from tqdm import tqdm

try:
    os.makedirs("out")
except FileExistsError:
    print("[WinError 183] 当文件已存在时，无法创建该文件。: 'out'")

img = cv2.imread("4f99128c-14b2-4046-8203-521902a42fde~0.jpg")
for i in tqdm(range(1001, 2001, 10)):
    full_img = []
    for col in range(5):
        row_img = []
        for row in range(2):
            text = "2022%04d" % i
            AddText = img.copy()
            cv2.putText(AddText, text, (1900, 675),
                        cv2.FONT_HERSHEY_SCRIPT_SIMPLEX, 1.25, (255, 255, 255), 2)
            i = i + 1
            row_img.append(AddText)
        row_img = np.hstack(row_img)
        full_img.append(row_img)
    full_img = np.vstack(full_img)
    cv2.imwrite('%s/2022%04d.jpg' % ("out", i - 10), full_img)
    # cv2.imwrite('%s/2022%04d.png' % ("out", i - 10), full_img, [cv2.IMWRITE_PNG_COMPRESSION, 0])
