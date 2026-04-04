const express = require('express');
const axios = require('axios');
const cors = require('cors');

const app = express();

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Environment variables
const PORT = process.env.PORT || 8000;
const MONOLITH_URL = process.env.MONOLITH_URL || 'http://monolith:8080';
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL || 'http://movies-service:8081';
const EVENTS_SERVICE_URL = process.env.EVENTS_SERVICE_URL || 'http://events-service:8082';
const GRADUAL_MIGRATION = process.env.GRADUAL_MIGRATION === 'true';
const MOVIES_MIGRATION_PERCENT = parseInt(process.env.MOVIES_MIGRATION_PERCENT || '50', 10);

// Logging middleware
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// Health endpoint
app.get('/health', (req, res) => {
  res.json({ status: true });
});

// Determine target service for movies based on gradual migration
function getMoviesTargetService() {
  if (!GRADUAL_MIGRATION) {
    return {
      url: MOVIES_SERVICE_URL,
      name: 'movies-service'
    };
  }

  const randomValue = Math.random() * 100;
  const shouldRouteToMoviesService = randomValue < MOVIES_MIGRATION_PERCENT;

  if (shouldRouteToMoviesService) {
    return {
      url: MOVIES_SERVICE_URL,
      name: 'movies-service'
    };
  } else {
    return {
      url: MONOLITH_URL,
      name: 'monolith'
    };
  }
}

// Proxy function
async function proxyRequest(req, res, targetUrl) {
  try {
    const url = `${targetUrl}${req.path}`;
    const queryString = new URLSearchParams(req.query).toString();
    const fullUrl = queryString ? `${url}?${queryString}` : url;

    console.log(`[PROXY] Forwarding ${req.method} ${fullUrl}`);

    const response = await axios({
      method: req.method,
      url: fullUrl,
      headers: {
        ...req.headers,
        host: new URL(targetUrl).host
      },
      data: req.body,
      validateStatus: () => true // Don't throw on any status code
    });

    // Forward status code
    res.status(response.status);

    // Forward headers (excluding some hop-by-hop headers)
    const headersToExclude = ['transfer-encoding', 'connection', 'keep-alive'];
    Object.keys(response.headers).forEach(key => {
      if (!headersToExclude.includes(key.toLowerCase())) {
        res.setHeader(key, response.headers[key]);
      }
    });

    // Forward response body
    res.send(response.data);

  } catch (error) {
    console.error(`[PROXY ERROR] ${error.message}`);
    if (error.response) {
      res.status(error.response.status).send(error.response.data);
    } else {
      res.status(500).json({ error: 'Proxy error', message: error.message });
    }
  }
}

// Movies endpoints - Gradual migration
app.all('/api/movies*', (req, res) => {
  const target = getMoviesTargetService();
  console.log(`[MIGRATION] Routing ${req.method} ${req.path} to ${target.name} (migration: ${MOVIES_MIGRATION_PERCENT}%)`);
  proxyRequest(req, res, target.url);
});

// Users endpoints - Route to monolith
app.all('/api/users*', (req, res) => {
  console.log(`[PROXY] Routing ${req.method} ${req.path} to monolith`);
  proxyRequest(req, res, MONOLITH_URL);
});

// Payments endpoints - Route to monolith
app.all('/api/payments*', (req, res) => {
  console.log(`[PROXY] Routing ${req.method} ${req.path} to monolith`);
  proxyRequest(req, res, MONOLITH_URL);
});

// Subscriptions endpoints - Route to monolith
app.all('/api/subscriptions*', (req, res) => {
  console.log(`[PROXY] Routing ${req.method} ${req.path} to monolith`);
  proxyRequest(req, res, MONOLITH_URL);
});

// Events endpoints - Route to events-service
app.all('/api/events*', (req, res) => {
  console.log(`[PROXY] Routing ${req.method} ${req.path} to events-service`);
  proxyRequest(req, res, EVENTS_SERVICE_URL);
});

// Catch-all for any other requests - Route to monolith
app.all('*', (req, res) => {
  console.log(`[PROXY] Routing ${req.method} ${req.path} to monolith (default)`);
  proxyRequest(req, res, MONOLITH_URL);
});

// Start server
app.listen(PORT, () => {
  console.log('='.repeat(50));
  console.log('CinemaAbyss API Gateway Proxy Service');
  console.log('='.repeat(50));
  console.log(`Server listening on port ${PORT}`);
  console.log(`Monolith URL: ${MONOLITH_URL}`);
  console.log(`Movies Service URL: ${MOVIES_SERVICE_URL}`);
  console.log(`Events Service URL: ${EVENTS_SERVICE_URL}`);
  console.log(`Gradual Migration: ${GRADUAL_MIGRATION}`);
  console.log(`Movies Migration Percent: ${MOVIES_MIGRATION_PERCENT}%`);
  console.log('='.repeat(50));
  console.log('Routing Rules:');
  console.log('  /health -> Health check');
  console.log('  /api/movies* -> Gradual migration (monolith <-> movies-service)');
  console.log('  /api/users* -> monolith');
  console.log('  /api/payments* -> monolith');
  console.log('  /api/subscriptions* -> monolith');
  console.log('  /api/events* -> events-service');
  console.log('  * -> monolith (default)');
  console.log('='.repeat(50));
});

module.exports = app;
