# IMPORTS
from io import BytesIO
from urllib import request
from PIL import Image
import onnxruntime as ort
import numpy as np

# ## Question 1
# 
# To be able to use this model, we need to know the name of the input and output nodes. 
# 
# What's the name of the output:
# 
# * **`output`**
# * `sigmoid`
# * `softmax`
# * `prediction`
sess = ort.InferenceSession("hair_classifier_v1.onnx")
print("output name:", sess.get_outputs()[0].name)

# ## Preparing the image
# 
# You'll need some code for downloading and resizing images. You can use 
# this code:
# 
# ```python
# from io import BytesIO
# from urllib import request
# 
# from PIL import Image
# 
# def download_image(url):
#     with request.urlopen(url) as resp:
#         buffer = resp.read()
#     stream = BytesIO(buffer)
#     img = Image.open(stream)
#     return img
# 
# 
# def prepare_image(img, target_size):
#     if img.mode != 'RGB':
#         img = img.convert('RGB')
#     img = img.resize(target_size, Image.NEAREST)
#     return img
# ```
# 
# For that, you'll need to have `pillow` installed:
# 
# ```bash
# pip install pillow
# ```

def download_image(url):
    with request.urlopen(url) as resp:
        buffer = resp.read()
    stream = BytesIO(buffer)
    img = Image.open(stream)
    return img


def prepare_image(img, target_size):
    if img.mode != 'RGB':
        img = img.convert('RGB')
    img = img.resize(target_size, Image.NEAREST)
    return img


# ## Question 2: Target size
# 
# Let's download and resize this image: 
# 
# https://habrastorage.org/webt/yf/_d/ok/yf_dokzqy3vcritme8ggnzqlvwa.jpeg
# 
# Based on the previous homework, what should be the target size for the image?
# 
# * 64x64
# * 128x128
# * **200x200**
# * 256x256

url = "https://habrastorage.org/webt/yf/_d/ok/yf_dokzqy3vcritme8ggnzqlvwa.jpeg"
img = download_image(url)
img_prepared = prepare_image(img, target_size=(200, 200))

# ## Question 3
# 
# Now we need to turn the image into numpy array and pre-process it. 
# 
# > Tip: Check the previous homework. What was the pre-processing 
# > we did there?
# 
# After the pre-processing, what's the value in the first pixel, the R channel?
# 
# * -10.73
# * **-1.073**
# * 1.073
# * 10.73

x = np.array(img_prepared).astype("float32") / 255
# x shape is (200, 200, 3)

# normalize
mean = np.array([0.485, 0.456, 0.406])
std = np.array([0.229, 0.224, 0.225])

x_norm = (x - mean) / std

print(f"R channel of the first pixel is the [0][0][0] element: {x_norm[0][0][0]}")

# ## Question 4
# 
# Now let's apply this model to this image. What's the output of the model?
# 
# * **0.09**
# * 0.49
# * 0.69
# * 0.89

x_input = np.transpose(x_norm, (2, 0, 1)).astype(np.float32)
x_input = x_input[None, :, :, :]  # add batch dimension

# Load model
sess = ort.InferenceSession("hair_classifier_v1.onnx")
input_name = sess.get_inputs()[0].name
output_name = sess.get_outputs()[0].name

# Run inference
pred = sess.run([output_name], {input_name: x_input})[0]

# Get probability
prob = pred[0][0]  # since shape is (1,1)
print("Probability of positive class:", prob)


# ## Prepare the lambda code 
# 
# Now you need to copy all the code into a separate python file. You will 
# need to use this file for the next two questions.
# 
# Tip: you can test this file locally with `ipython` or Jupyter Notebook 
# by importing the file and invoking the function from this file.  

# ## Docker 
# 
# For the next two questions, we'll use a Docker image that we already 
# prepared. This is the Dockerfile that we used for creating the image:
# 
# ```docker
# FROM public.ecr.aws/lambda/python:3.13
# 
# COPY hair_classifier_empty.onnx.data .
# COPY hair_classifier_empty.onnx .
# ```
# 
# Note that it uses Python 3.13.
# 
# The docker image is published to [`agrigorev/model-2024-hairstyle:v3`](https://hub.docker.com/r/agrigorev/model-2024-hairstyle/tags).
# 
# A few notes:
# 
# * The image already contains a model and it's not the same model
#   as the one we used for questions 1-4.

