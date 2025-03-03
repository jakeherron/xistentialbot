# xistentialbot: Automated Existential Tweet Generator and Poster

## Overview
`xistentialbot` is a Python-based automation tool designed to schedule and post existential, thought-provoking tweets to the X platform, leveraging Google Sheets for content management and Google Cloud Platform (GCP) for serverless deployment. This project showcases my ability to integrate APIs, deploy cloud-based solutions, and craft concise, impactful content—skills directly applicable to prompt engineering and AI-driven development roles. Currently, it automates the posting of manually curated tweets, with plans to incorporate AI-generated content in the future. The project draws inspiration from profound works exploring human nature, technology, and societal absurdity.

## Project Goals
- Automate the daily posting of existential reflections on human nature, technology, and modern absurdity.
- Demonstrate proficiency in Python scripting, cloud computing, and API integration.
- Build a scalable, serverless solution using GCP tools.
- Prepare a foundation for AI-driven content generation, aligning with prompt engineering objectives.

## Features
- **Scheduled Posting**: Posts tweets at user-defined times (e.g., 08:00, 15:30, 20:00 PST) using GCP Cloud Scheduler.
- **Dynamic Content Management**: Reads and updates tweet statuses via Google Sheets.
- **Serverless Deployment**: Hosted on Google Cloud Functions for zero-maintenance operation.
- **Error Handling**: Implements robust logging to monitor and debug issues.
- **Future AI Integration**: Planned enhancement to generate original tweets using an AI model (e.g., OpenAI).

## Tech Stack
- **Languages**: Python 3.9
- **Libraries**:
  - `tweepy`: For X API interaction.
  - `gspread` and `oauth2client`: For Google Sheets integration.
  - `pandas`: For data manipulation.
  - `pytz`: For time zone handling.
  - `python-dotenv`: For secure local environment variable management.
- **Cloud Platform**: Google Cloud Platform (Functions, Scheduler, Run)
- **APIs**: X API, Google Sheets API

## Development Process
### Planning
- Identified a need to automate existential tweet posting to explore automation and cloud skills.
- Chose X for its social reach and Google Sheets for flexible, real-time content updates.

### Implementation
- Developed a Python script to parse Google Sheet data, post to X via the API, and update statuses.
- Deployed the script as a serverless function on GCP Cloud Functions.
- Configured Cloud Scheduler to trigger the function hourly, ensuring timely posts within a 1-hour window.
- Integrated secure credential management using environment variables.

### Challenges and Solutions
- **Time Zone Alignment**: Resolved discrepancies between PST and UTC using `pytz`, ensuring accurate scheduling.
- **Deployment Errors**: Debugged a 500 Internal Server Error by adjusting the function signature to accept HTTP requests.
- **Security**: Transitioned from hardcoded API keys to environment variables for GitHub compatibility, maintaining deployed functionality.

## Setup Instructions
1. **Clone the Repository**:
   - `git clone https://github.com/jakeherron/xistentialbot.git`
2. **Install Dependencies**:
   - Create a virtual environment: `python -m venv venv`
   - Activate it: `source venv/bin/activate` (Linux/Mac) or `venv\Scripts\activate` (Windows)
   - Install requirements: `pip install -r requirements.txt`
3. **Configure Environment Variables**:
   - Create a `.env` file in the project root:
X_BEARER_TOKEN=your_bearer_token X_API_KEY=your_api_key X_API_SECRET=your_api_secret X_ACCESS_TOKEN=your_access_token X_ACCESS_TOKEN_SECRET=your_access_token_secret

- Obtain these from the X Developer Portal and Google Cloud IAM.
4. **Local Testing**:
- Run: `python main.py` to test locally (ensure Google Sheets credentials are set up).
5. **Deployment**:
- Deploy to GCP: `gcloud functions deploy post_to_x --runtime python39 --trigger-http --allow-unauthenticated --region us-central1 --source . --entry-point post_to_x --set-build-env-vars "GOOGLE_FUNCTIONS_FRAMEWORK_VERSION=2.1.0" --set-env-vars "LOG_EXECUTION_ID=true" --verbosity debug`
- Set up Cloud Scheduler: `gcloud scheduler jobs create http post-to-x-job --schedule="0 * * * *" --uri="https://us-central1-xistentialbot-free.cloudfunctions.net/post_to_x" --http-method=GET --location=us-central1`

## Results
- Successfully deployed and tested on March 2, 2025, with initial posts (e.g., "automation test ¯\_(ツ)_/¯ i am alive" and "Freedom, our sacred chant...") confirming functionality at 16:00 PST.
- Automates approximately 91 tweets per month (3 posts/day at 08:00, 15:30, 20:00 PST) through September 18, 2025.
- Handles ~200 scheduled posts currently, with scalability for additional content.

## Skills Demonstrated
- **Prompt Engineering**: Crafted 200+ existential tweets, optimizing for brevity and philosophical depth within X’s 280-character limit.
- **Python Development**: Utilized libraries for API interaction, data handling, and logging.
- **Cloud Computing**: Mastered GCP Functions and Scheduler for serverless automation.
- **API Integration**: Seamlessly connected X and Google Sheets APIs.
- **Problem-Solving**: Overcame deployment and time zone challenges with effective debugging.

## Inspirations
This project draws inspiration from the following works, which shaped its existential and philosophical tone:
- *Amusing Ourselves to Death* – Neil Postman (exploration of media’s impact on society)
- *Brave New World* – Aldous Huxley (dystopian view of technology and control)
- *1984* – George Orwell (surveillance and loss of freedom)
- *Technopoly* – Neil Postman (technology’s dominance over culture)
- *The Stranger* – Albert Camus (absurdity of human existence)
- Works by Dostoyevsky (e.g., *Notes from Underground*, *Crime and Punishment*, *The Brothers Karamazov*) (depth of human psyche and morality)
- *The Human Animal* – Desmond Morris (biological roots of human behavior)

## Future Enhancements
- **AI Integration**: Plan to incorporate an AI model (e.g., OpenAI) to generate original existential tweets, log them in the Sheet, and automate posting—enhancing prompt engineering capabilities.
- **Performance Optimization**: Explore filtering or batching for larger datasets.
- **Monitoring**: Add alerts for failures via GCP Cloud Monitoring.

## Acknowledgments
- Built with guidance from xAI’s Grok 3, leveraging its troubleshooting and optimization expertise.
- Inspired by existential philosophy and the potential of automation in content creation.

## License
[MIT License](LICENSE) (or choose your preferred open-source license—create a `LICENSE` file if desired).

## Contact
- GitHub: [jakeherron](https://github.com/jakeherron)
- Email: thefinalburden@proton.me
- LinkedIn: (https://www.linkedin.com/in/jacob-herron-308810221/)
