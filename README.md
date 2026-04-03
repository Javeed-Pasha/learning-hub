# Learning Hub

A personal knowledge base and learning website — built to document and share what I learn about data engineering, ETL, cloud, Python, SQL, and more.

## Live Site

> After pushing to GitHub, enable **GitHub Pages** (Settings → Pages → Source: `main` branch, `/ (root)`) and your site will be live at:
> `https://<your-username>.github.io/learning-hub/`

## Structure

```
learning-hub/
├── index.html          # Landing page with links to all topics
├── pages/              # Individual content pages
│   └── informatica-powercenter-101.html
├── README.md
└── .gitignore
```

## Adding New Content

1. Create a new `.html` file in the `pages/` folder
2. Add a card linking to it in `index.html` inside the `<div id="contentGrid">` section
3. Commit and push — GitHub Pages will update automatically

## Tech

- Pure HTML + CSS (no frameworks, no build step)
- Dark theme with IBM Plex fonts
- Fully responsive
- Hosted free on GitHub Pages
