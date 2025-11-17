# ML Zoomcamp 8.1 - Fashion Classification

## 🎯 Overview
This session introduces **neural networks and deep learning**, shifting from traditional **tabular data** to **image data**.  
Unlike previous lessons that used models like Linear or Logistic Regression and Tree-based methods for CSV data, this session focuses on **visual recognition tasks**.

## 👕 Use Case: Fashion Image Classification
We will build a **multi-class image classification model** for online fashion listings.

![](./imgs/ml-8-1/1.png)
![](./imgs/ml-8-1/2.png)

### Scenario:
- Users upload images (e.g., of clothing items) to an online store.
- The system automatically predicts the **category** (e.g., T-shirt, dress, shoes).
- This helps users quickly list their products by suggesting the right category.

## 🧩 Dataset: Fashion Classification Dataset
- Contains **~5,000 images** of clothing items across **20+ categories**.
- For simplicity, a **subset of 10 popular categories** is used.
- Dataset already includes **train**, **validation**, and **test** splits.
- Each folder represents a specific category (e.g., *t-shirts*, *pants*, *hats*).

✅ Example Categories:
- T-shirts  
- Skirts  
- Shoes  
- Coats  
- Long sleeves  
- Outwear  

No manual data splitting is needed — the dataset structure is prearranged.

## 🧪 Goal
Train a **Neural Network model** that predicts the correct fashion category from an uploaded image.  
This model will power a **fashion classification service**, providing instant category suggestions for uploaded images.

## 🧰 Tools & Frameworks
- **TensorFlow** and **Keras** for model building and training.
- The workflow includes:
  - Loading and preprocessing image data
  - Defining and training neural network models
  - Evaluating classification accuracy
  - Applying **transfer learning** from pre-trained models

## 🧱 Neural Network Focus
While the session is practical, it briefly introduces key theoretical ideas:
- **How neural networks work**
- **Convolutional Neural Networks (CNNs)** for image data
- **Layers, parameters, and activation functions**
- **Regularization and data augmentation** for better generalization

📚 For a deeper theoretical understanding, the instructor recommends **Stanford’s CS231n course**, which provides detailed explanations and lecture notes on CNNs and computer vision.

## 🔄 Transfer Learning
A major concept introduced is **transfer learning**, where:
- A pre-trained model is reused and fine-tuned for a new task.
- Saves training time and improves performance with limited data.

## 🧩 Key Learning Topics
1. Neural networks for image data  
2. TensorFlow & Keras model setup  
3. Convolutional layers overview  
4. Transfer learning for efficient training  
5. Regularization and data augmentation  
6. Practical training with minimal theory  

---

# ML Zoomcamp - 8.1b Setting up the Environment on Saturn Cloud

## 🎯 Overview
This transcript explains how to set up a **GPU-enabled Jupyter notebook environment** for training neural networks, using **Saturn Cloud** as an alternative to AWS SageMaker due to recent GPU access limitations on AWS.

## 🌩️ Why Saturn Cloud?
- AWS now requires **manual GPU approval**, often declined.
- Saturn Cloud provides **easy, instant access** to Jupyter notebooks with GPUs.
- A special link (with UTM parameters) is provided to track sign-ups.

## 📝 Setting Up the Environment

### 1. **Sign Up / Log In**
- Use the provided link to create or access your Saturn Cloud account.

### 2. **Optional: Configure GitHub SSH Access**
- Useful if you want to push your notebooks directly to GitHub.
- Steps include:
  - Creating an SSH key locally.
  - Adding its content as a **Secret** in Saturn Cloud.
  - Ensuring it ends with a newline.
  - Attaching the secret file to your instance under `.ssh/default`.

### 3. **Create a GPU Notebook Resource**
- Choose the **TensorFlow + Python** template.
- Name the project (e.g., *classification-ml-zoomcamp*).
- Edit resource settings to add a `requirements.txt` (e.g., to install `scipy` or other custom packages).
- Save configuration.

### 4. **Start the Instance**
- GPU resources may take **2–5 minutes** to boot.
- Launch the **Jupyter Notebook interface** once ready.

## 🔐 Verify GitHub Access (Optional)
- Open a terminal inside the notebook and test:
  - SSH connection to GitHub.
  - Git initialization, adding, committing, and pushing files.

## 🧵 Download the Dataset
- Clone the clothing dataset used in the module.
- You can clone via **HTTPS** or **SSH**, depending on whether you configured SSH keys.

## 🧪 Validate TensorFlow Installation
- Create a new notebook.
- Import TensorFlow and check its version.
- GPU-enabled TensorFlow should be available.

## 🏋️ Use the Notebook for the Module & Homework
- Follow along with the videos using this environment.
- Run all training, experiments, and homework assignments on the GPU-backed notebook.

---

# ML Zoomcamp 8.2 - Tensorflow and Keras

### 🌐 What Are TensorFlow and Keras?
- **TensorFlow** is an open-source, end-to-end machine learning framework focused primarily on **deep learning**.
- **Keras** is a **high-level API inside TensorFlow** that simplifies building, training, and using neural networks.
- Modern TensorFlow (2.x+) includes Keras directly:  
  `tensorflow.keras`

![](./imgs/ml-8-1/1.png)

### 🛠 Installing TensorFlow
- TensorFlow is **not included** with Anaconda; it must be installed manually.
- Install using:
  - `pip install tensorflow`
  - or inside notebooks: `!pip install tensorflow -y`
- GPU installation is more complex; many prefer cloud environments like **Amazon SageMaker** that come preconfigured.

### 📦 Importing TensorFlow & Keras
- Typical usage:
  - `import tensorflow as tf`
  - `from tensorflow import keras`

### 🖼 Loading and Working With Images
- The dataset contains images (Train/Validation/Test splits).
- TensorFlow/Keras provides utility functions to load images, e.g.:
  - `keras.preprocessing.image.load_img(...)`
- Older tutorials may use `from keras.preprocessing.image import load_img`;  
  in TF 2.x, prefix with `tensorflow.`

### 🔧 Image Resizing for Neural Networks
- Neural networks expect **fixed-size** images, such as:
  - 299×299  
  - 224×224  
  - 150×150  
- Images are resized using the `target_size` argument when loading.

### 🧩 How Images Are Represented
- Internally, images are stored as **arrays with 3 channels**:  
  **Red, Green, Blue (RGB)**.
- Pixel values range from **0 to 255** (8-bit integers).
- The array shape for a resized 150×150 image is:  
  **150 × 150 × 3**

![](./imgs/ml-8-1/2.png)

### 🔄 Converting Images to NumPy Arrays
- Keras returns a PIL image internally.
- It can be converted to a NumPy array to feed into models.
- Each element corresponds to a pixel’s RGB values.

---

# ML Zoomcamp 8.3 - Pre-Trained Convolutional Neural Networks

### 🌍 Overview of Pre-Trained CNNs
- Pre-trained convolutional neural networks (CNNs) are deep learning models trained on **large image datasets**, often containing **1,000 classes** (such as ImageNet).
- These models can recognize a wide variety of objects and are widely used for **image classification**, **feature extraction**, and **transfer learning**.

### 🖼 ImageNet and Pretrained Models
- ImageNet is a large-scale dataset containing millions of images organized into 1,000 categories.
- Pre-trained CNNs (e.g., ResNet, VGG, Inception) have learned rich visual features from this dataset.
- These models can output a **1,000-dimensional vector**, where each value represents the predicted probability of an ImageNet class.

### 🧩 How Pre-Trained Models Work
- Input images are transformed and resized to match the model’s expected format.
- After passing an image through the model, the resulting vector can be interpreted to determine what the network recognizes in the image.
- The output probabilities help identify the most likely object the model “sees.”

### 🔧 Using Pre-Trained Models
- Pretrained CNNs remove the need to train a model from scratch.
- They can be used directly for:
  - Image classification  
  - Feature extraction (for downstream tasks)  
  - Transfer learning (by fine-tuning on a smaller dataset)  

### 🎯 Key Takeaway
Pre-trained CNNs provide powerful, ready-to-use tools for visual recognition tasks, enabling rapid experimentation and high performance with minimal data and computation.

---

# ML Zoomcamp 8.4 - Convolutional Neural Networks

## Overview
Convolutional Neural Networks (CNNs) are specialized neural networks designed for image tasks. They extract visual features through convolutional layers and make predictions using dense layers.

## 1. CNN Structure
A CNN processes an image through multiple layers:
- **Convolutional layers** – extract patterns and features.
- **Dense (fully connected) layers** – use extracted features to classify the image.

The model transforms the input image into a **vector representation**, which is then used for prediction.

![](./imgs/ml-8-4/1.png)
![](./imgs/ml-8-4/2.png)
![](./imgs/ml-8-4/3.png)
![](./imgs/ml-8-4/4.png)
![](./imgs/ml-8-4/5.png)
![](./imgs/ml-8-4/6.png)
![](./imgs/ml-8-4/7.png)
![](./imgs/ml-8-4/8.png)
![](./imgs/ml-8-4/9.png)
![](./imgs/ml-8-4/10.png)
![](./imgs/ml-8-4/11.png)
![](./imgs/ml-8-4/12.png)
![](./imgs/ml-8-4/13.png)
![](./imgs/ml-8-4/14.png)
![](./imgs/ml-8-4/15.png)

## 2. Convolutional Layers
### Filters
- Small learnable matrices (e.g., 5×5).
- Detect simple shapes like edges or lines.
- Sliding a filter over the image produces a **feature map** showing similarity scores.

### Feature Maps
- Each filter generates one feature map.
- High values indicate strong similarity between the filter and a specific image region.

### Stacking Convolution Layers
- Layer 1 learns simple patterns (edges, stripes).
- Layer 2 learns combinations (corners, curves).
- Layer 3 learns complex shapes (e.g., sleeves, full components).
- Deeper layers capture higher-level concepts.

## 3. From Image to Vector
After multiple convolutional layers, the image is transformed into a **1D vector** (e.g., length 1024 or 2048).  
This vector encodes:
- Shapes  
- Colors  
- Spatial structures  
- Object parts  

It becomes the input to dense layers.

## 4. Dense Layers
Dense layers:
- Connect every input unit to every output unit.
- Perform a **matrix multiplication** between inputs and learned weights.
- Convert the vector representation into class probabilities.

## 5. Classification
### Binary Classification
To detect if the image is a *t-shirt or not*:
- Use logistic regression (sigmoid output).
- Output = probability the image belongs to the class.

### Multi-Class Classification
For classes such as:
- Shirt
- T-shirt
- Dress

Use:
- One dense layer with **softmax**, producing one probability per class.

## 6. End-to-End CNN Process
1. **Input image**  
2. **Convolutions** extract increasingly complex visual features  
3. **Flattening** produces a feature vector  
4. **Dense layers** interpret the vector  
5. **Softmax output** assigns probabilities to each class  

CNNs learn filters and weights automatically during training.

---

# ML Zoomcamp 8.5 - Transfer Learning

## Overview
Transfer learning allows us to reuse the convolutional layers of a neural network already trained on a large dataset (e.g., ImageNet) and adapt it to a new task with fewer images and faster training.

![](./imgs/ml-8-5/1.png)
![](./imgs/ml-8-5/2.png)
![](./imgs/ml-8-5/3.png)
![](./imgs/ml-8-5/4.png)
![](./imgs/ml-8-5/5.png)
![](./imgs/ml-8-5/6.png)
![](./imgs/ml-8-5/7.png)

## Why Transfer Learning Works
- Convolutional layers learn **generic visual filters** (edges, textures, shapes) that are useful for many tasks.
- Dense (fully-connected) layers are **task-specific** (e.g., ImageNet has 1000 classes).
- We keep the **pre-trained convolutional base** and replace the **dense layers** with new ones tailored to our dataset.

## Data Preparation
- Use `ImageDataGenerator` to load images from directories and apply preprocessing.
- Define:
  - **Target size** (e.g., 150×150 for faster experimentation)
  - **Batch size** (e.g., 32)
- Data generator outputs:
  - `x` — batch of images
  - `y` — one-hot encoded labels for multi-class classification

## Pre-Trained Base Model
- Use a model such as **Xception** with:
  - `weights="imagenet"`
  - `include_top=False` (discard dense layers)
  - `trainable=False` (freeze convolutional layers)
- The output of the base model for each image is a **feature map** (e.g., 5×5×2048).

## Converting Feature Maps to Vectors
- The feature map is 3-dimensional; we convert it into a single vector using:
  - **Global Average Pooling 2D**
  - Computes the average value of each filter channel
  - Produces a 1D vector of size equal to the number of filters (e.g., 2048)

## Building the New Classifier (Functional API)
Pipeline:
1. Input layer receives images.
2. Input passes through the frozen base model → produces feature maps.
3. Global Average Pooling converts feature maps → vector representation.
4. Dense layer with softmax produces predictions for **10 classes**.

## Model Training
- The dense layer starts with **random weights**, so predictions are meaningless until training.
- Use an **optimizer**, typically:
  - **Adam** with a chosen learning rate (e.g., 0.01)
- Optimizer updates parameters of the new dense layers using gradient descent.

## Key Takeaways
- Transfer learning saves time and data by reusing powerful pre-learned features.
- Only the top classification layers need training.
- Image generators simplify loading and preprocessing.
- Pooling transforms feature maps into vectors usable by dense layers.

---

# ML Zoomcamp 8.6 - Adjusting the Learning Rate

## Overview
This lesson explains how to choose an appropriate learning rate for training a neural network. It builds on the previous lesson, where a transfer learning model reached ~81% accuracy. The goal is to understand how learning rate affects performance and how to select the best value through experimentation.

![](./imgs/ml-8-6/1.png)

## Understanding Learning Rate
The learning rate controls **how fast a model learns**.  
A real-world analogy is reading books at different speeds:

- **High learning rate** = reading many books very quickly  
  - Fast but superficial learning  
  - Leads to **overfitting** or unstable training
- **Medium learning rate** = reading four books per year  
  - Balanced pace, good retention  
  - Best overall performance
- **Low learning rate** = reading one book per year extremely slowly  
  - Very accurate but too slow  
  - Leads to **underfitting**

The goal is to find the **“sweet spot”** where learning is fast enough but still stable.

## Why Learning Rate Matters
- **Too high** → model learns erratically, forgets information, high risk of overfitting  
- **Too low** → model learns extremely slowly, may never reach good performance  
- **Just right** → efficient learning with good generalization on validation data

This concept applies not only to neural networks but also to models like gradient boosting.

## Experimenting with Different Learning Rates
To find the best learning rate:
1. Reuse the model architecture from the previous lesson.
2. Wrap model creation in a function parameterized by learning rate.
3. Train several models with different learning rates.
4. Capture training history for each setting.
5. Compare training and validation accuracy curves.

The model is tested with several learning rate values, including very small, medium, and large values.

## Results of the Learning Rate Comparison
- Very small learning rates learn **too slowly**, reaching low accuracy after many iterations.
- Extremely high learning rates are unstable and perform poorly.
- After reviewing accuracy plots, the best performance is consistently achieved with:
  
**Learning rate = 0.01**

This value provides:
- Higher validation accuracy  
- Better training behavior  
- Smaller gap between training and validation performance  

## Key Takeaways
- Proper learning rate selection is critical for stable, effective model training.
- You should **experiment with multiple values** and compare validation performance.
- The selected learning rate for future lessons will be **0.01**.

---

# ML Zoomcamp 8.7 - Checkpointing

## Overview
This lesson introduces **checkpointing**, a method for saving model weights during training—especially when performance fluctuates across epochs. Instead of saving only the final model (which may be worse), checkpointing allows saving the **best-performing version**.

![](./imgs/ml-8-6/1.png)
![](./imgs/ml-8-6/2.png)
![](./imgs/ml-8-6/3.png)

## Why Checkpointing Matters
- Validation accuracy often **oscillates** during training.
- The final epoch may not produce the best model.
- Checkpointing preserves the model from the **epoch with highest validation accuracy**.
- Prevents losing a good model due to later overfitting or performance drops.

Example scenario:
- Best model might appear at epoch 8.
- Training continues to epoch 10.
- Without checkpointing, only the epoch 10 model is saved—even if it performs worse.

## How Checkpointing Works
During training:
1. After each epoch, the model is evaluated on validation data.
2. A **callback** checks the validation metric.
3. If the metric improves, the model is saved.

Keras uses **callbacks** to run custom logic after each epoch.

## ModelCheckpoint Callback
The `ModelCheckpoint` callback allows:
- **Saving model weights** to files.
- Using filenames that include:
  - Epoch number  
  - Validation accuracy  
- Choosing to save:
  - **All epochs**  
  - **Only the best epoch** (recommended)

Key configuration options:
- `save_best_only=True` → save only when validation accuracy improves  
- `monitor="val_accuracy"` → metric to track  
- `mode="max"` → higher accuracy is better  
- File naming uses formatting tokens for epoch and metric values.

## What Happens During Training
- At each epoch:
  - Validation accuracy is calculated.
  - If it is higher than all previous epochs, the model is saved.
- When accuracy gets worse, no new file is created.
- Only the best model remains.

In the example:
- Accuracy improved until **epoch 6**.
- From epochs 7 onward, accuracy decreased.
- Only the epoch 6 model was saved.

The best model achieved **83.6% accuracy**.

## Conclusion
Checkpointing ensures:
- The best version of the model is preserved.
- Training noise or temporary decline won't overwrite optimal weights.
- You can safely train for many epochs without worrying about losing good performance.

---

# ML Zoomcamp 8.8 - Adding More Layers

## Overview
This lesson explains how to enhance a pre-trained convolutional neural network by adding **extra dense layers** after the vector representation step. The goal is to increase model capacity and potentially improve accuracy for the image classification task.

## Base Model Recap
- The model uses a **pre-trained ImageNet CNN** with `include_top=False`.
- The convolutional base outputs a **vector representation** of images.
- A single dense layer (the output layer) maps this vector to class predictions.  
  This version is referred to as **Model v1**.

## Adding an Inner Dense Layer
To make the network more expressive:
- Insert an **additional dense layer** between the vector representation and the output layer.
- This new layer performs **intermediate processing** and may help the network learn more meaningful patterns.
- Example architecture change:
  - Vector representation → **Inner Dense Layer** → Output layer
- The size of this inner layer (e.g., 10, 100, 1000) becomes a **hyperparameter** to tune.

## Activation Functions
- Neural network layers need **activation functions** to introduce non-linearity.
- The output layer implicitly uses softmax (via logits + softmax combination).
- The new inner layer requires an activation; the lesson uses **ReLU**, a standard choice for hidden layers.
- ReLU:
  - Output is 0 for negative inputs.
  - Output is linear for positive inputs.
  - Simple, fast, and widely used.

Other activations discussed:
- **Sigmoid**
- **Softmax**  
(Usually for output layers, not hidden layers.)

## Training the Enhanced Model
- The model is trained with different inner layer sizes (10, 100, 1000).
- Learning rate tuning reuses results from earlier lessons.
- Training is run for 10 epochs per configuration.

### Monitoring GPU Usage
- GPU utilization is monitored using `nvidia-smi`.
- High utilization (>90%) indicates efficient resource use.
- If utilization drops, performance tuning may be required.

## Results & Interpretation
- Surprisingly, adding the extra layer **did not improve accuracy**.
- All tested sizes produced results similar to:
  - The original model without an extra dense layer.
  - Each other (no meaningful differences).
- This suggests:
  - The dataset/model might not benefit from added complexity.
  - The network may need **longer training** or **regularization techniques** for the benefits to appear.

## Takeaways
- Adding additional dense layers makes a neural network **more powerful**, but not always **better**.
- More parameters mean more complexity and potential overfitting.
- The extra layer is kept for now because the next lesson introduces:
  - **Regularization**
  - **Dropout**
  - …and these techniques will be applied to this inner layer.

---

# 8.9 https://www.facebook.com/stories/1614571798619375/UzpfSVNDOjI1MDU5NTk0NTE3MDI0NDY0/?bucket_count=9&source=story_tray
