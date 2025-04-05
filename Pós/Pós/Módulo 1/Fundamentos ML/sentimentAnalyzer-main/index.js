import express from 'express';
import bodyParser from 'body-parser';
import { pipeline } from '@xenova/transformers';
import path from 'path';
import { fileURLToPath } from 'url';

// Configurações para ES6 modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Inicializa o Express
const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Configura o EJS como motor de templates
app.set('view engine', 'ejs');

// Rota principal
app.get('/', (req, res) => {
    res.render('index', { result: null });
});

// Rota para análise de sentimento
app.post('/analyze', async (req, res) => {
    try {
        const { text } = req.body;
        let pipe = await pipeline('sentiment-analysis', 'Xenova/distilbert-base-uncased-finetuned-sst-2-english');
        let out = await pipe(text);
        res.render('index', { result: out });
    } catch (error) {
        res.render('index', { result: `Error: ${error.message}` });
    }
});

// Inicia o servidor
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
