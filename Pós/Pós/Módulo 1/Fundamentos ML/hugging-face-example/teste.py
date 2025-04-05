from transformers import AutoModelForSequenceClassification, AutoTokenizer
import torch

nome_repo = "iurymiguel/curso-fiap"

tokenizer = AutoTokenizer.from_pretrained(nome_repo)
model = AutoModelForSequenceClassification.from_pretrained(nome_repo, num_labels=2)

class_labels = {0: "Negative", 1: "Positive"}

text = "Estou muito feliz com o resultado do meu trabalho!"
inputs = tokenizer(text, return_tensors="pt")

with torch.no_grad():
    outputs = model(**inputs)

logits = outputs.logits
predicted_class_id = torch.argmax(logits).item()

predicted_class_label = class_labels[predicted_class_id]

print(f"Texto: {text}")
print(f"Predicted class: {predicted_class_label}")

#Esse é um modelo sem treinamento, apenas para teste. Por isso só dá Negative.