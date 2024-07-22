# ASW (AssetWise)

## Sobre a Companhia

A **AssetWise (ASW)** é uma companhia dedicada a fornecer soluções inovadoras e inteligentes para a gestão de ativos financeiros. Nossa missão é capacitar indivíduos e organizações a tomar decisões financeiras informadas e eficientes, utilizando tecnologia de ponta e análise de dados avançada.

## Missão

Nossa missão é transformar a forma como os ativos financeiros são gerenciados, proporcionando ferramentas e insights que ajudam nossos clientes a alcançar seus objetivos financeiros com confiança e segurança.

## Visão

Ser a plataforma líder mundial em gestão de ativos financeiros, reconhecida pela excelência em inovação, segurança e satisfação do cliente.

## Valores

- **Inovação:** Estamos comprometidos em continuamente buscar novas tecnologias e métodos para melhorar nossos serviços.
- **Integridade:** Agimos com transparência e ética em todas as nossas interações e transações.
- **Excelência:** Nos esforçamos para fornecer serviços da mais alta qualidade.
- **Segurança:** Priorizamos a proteção dos dados e ativos de nossos clientes.
- **Satisfação do Cliente:** Nos dedicamos a entender e atender às necessidades de nossos clientes.

## Produtos e Serviços

### 1. Gestão de Portfólio

Ferramentas avançadas para monitorar, analisar e otimizar carteiras de investimentos, garantindo o melhor desempenho possível.

### 2. Análise de Risco

Soluções para identificar, medir e mitigar riscos financeiros, ajudando nossos clientes a proteger seus investimentos.

### 3. Consultoria Financeira

Serviços personalizados de consultoria para ajudar indivíduos e empresas a desenvolver estratégias financeiras sólidas e sustentáveis.

### 4. Educação Financeira

Recursos educativos para capacitar nossos clientes com conhecimento financeiro, incluindo webinars, workshops e artigos.

## Tecnologias Utilizadas

- **Frontend:** Next.js, React, Tailwind CSS
- **Backend:** Flask, Pandas, SQLAlchemy
- **Banco de Dados:** PostgreSQL
- **Outras Tecnologias:** Docker, Kubernetes, CI/CD

## Instalação e Configuração

### Pré-requisitos

- [Node.js](https://nodejs.org/)
- [Python 3.8+](https://www.python.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Git](https://git-scm.com/)

### Instruções

1. **Clone o repositório:**

   ```bash
   git clone https://github.com/faustostangler/ASW.git
   cd ASW

2. **Configuração do Frontend:**

    cd frontend
    npm install
    npm run dev

O frontend estará disponível em http://localhost:3000.


3. **Configuração do Backend:**

    cd backend
    python -m venv .venv
    .\.venv\Scripts\activate
    pip install -r requirements.txt

    # Configuração do banco de dados
    flask db init
    flask db migrate -m "Initial migration"
    flask db upgrade

    # Executar o servidor
    python run.py

O backend estará disponível em http://localhost:5000.

### Contribuições
Contribuições são bem-vindas! Por favor, siga os passos abaixo para contribuir:

    Faça um fork do projeto
    Crie uma branch para sua feature (git checkout -b feature/nova-feature)
    Commit suas mudanças (git commit -m 'Adiciona nova feature')
    Faça o push para a branch (git push origin feature/nova-feature)
    Abra um Pull Request


### Licença
Este projeto está licenciado sob a MIT License.

Contato
Para mais informações, entre em contato comigo:
Email: fausto.stangler@gmail.com
