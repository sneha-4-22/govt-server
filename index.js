const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Client, Databases } = require('node-appwrite');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// Initialize Appwrite
const client = new Client();
client
  .setEndpoint(process.env.APPWRITE_ENDPOINT || 'https://fra.cloud.appwrite.io/v1')
  .setProject(process.env.APPWRITE_PROJECT_ID || '6906074b000e78a3a942');

const databases = new Databases(client);
const DATABASE_ID = process.env.DATABASE_ID || '690609420002bfd26330';
const COLLECTION_ID = process.env.COLLECTION_ID || 'mgnrega_data';

const GOV_API_KEY = process.env.GOV_API_KEY || '579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b';
const GOV_API_URL = 'https://api.data.gov.in/resource/ee03643a-ee4c-48c2-ac30-9f2ff26ab722';

// Sync data from data.gov.in to Appwrite
app.post('/api/sync/:district', async (req, res) => {
  const { district } = req.params;
  
  try {
    // Fetch from government API
    const response = await axios.get(GOV_API_URL, {
      params: {
        'api-key': GOV_API_KEY,
        'format': 'json',
        'filters[state_name]': 'Uttar Pradesh',
        'filters[district_name]': district,
        'limit': 100
      },
      timeout: 15000
    });

    const records = response.data.records || [];
    let synced = 0;

    // Store in Appwrite
    for (const record of records) {
      try {
        await databases.createDocument(
          DATABASE_ID,
          COLLECTION_ID,
          'unique()',
          {
            district: record.district_name || district,
            state: 'Uttar Pradesh',
            fin_year: record.fin_year || '2024-25',
            total_households: parseInt(record.total_households) || 0,
            job_cards_issued: parseInt(record.job_cards_issued) || 0,
            employment_demanded: parseInt(record.employment_demanded) || 0,
            employment_provided: parseInt(record.employment_provided) || 0,
            persondays_generated: parseInt(record.persondays_generated) || 0,
            avg_days_per_household: parseInt(record.avg_days_per_household) || 0,
            works_completed: parseInt(record.works_completed) || 0,
            expenditure_cr: parseFloat(record.total_expenditure_cr) || 0,
            last_synced: new Date().toISOString()
          }
        );
        synced++;
      } catch (err) {
        console.log('Duplicate or error:', err.message);
      }
    }

    res.json({ 
      success: true, 
      message: `Synced ${synced} records for ${district}`,
      total: records.length 
    });

  } catch (error) {
    console.error('Sync error:', error.message);
    res.status(500).json({ 
      error: 'Sync failed',
      message: error.message 
    });
  }
});

// Batch sync all districts
app.post('/api/sync-all', async (req, res) => {
  const districts = ['Agra', 'Lucknow', 'Varanasi', 'Kanpur Nagar', 'Allahabad'];
  const results = [];

  for (const district of districts) {
    try {
      // Fetch from government API
      const response = await axios.get(GOV_API_URL, {
        params: {
          'api-key': GOV_API_KEY,
          'format': 'json',
          'filters[state_name]': 'Uttar Pradesh',
          'filters[district_name]': district,
          'limit': 100
        },
        timeout: 15000
      });

      const records = response.data.records || [];
      let synced = 0;

      // Store in Appwrite
      for (const record of records) {
        try {
          await databases.createDocument(
            DATABASE_ID,
            COLLECTION_ID,
            'unique()',
            {
              district: record.district_name || district,
              state: 'Uttar Pradesh',
              fin_year: record.fin_year || '2024-25',
              total_households: parseInt(record.total_households) || 0,
              job_cards_issued: parseInt(record.job_cards_issued) || 0,
              employment_demanded: parseInt(record.employment_demanded) || 0,
              employment_provided: parseInt(record.employment_provided) || 0,
              persondays_generated: parseInt(record.persondays_generated) || 0,
              avg_days_per_household: parseInt(record.avg_days_per_household) || 0,
              works_completed: parseInt(record.works_completed) || 0,
              expenditure_cr: parseFloat(record.total_expenditure_cr) || 0,
              last_synced: new Date().toISOString()
            }
          );
          synced++;
        } catch (err) {
          console.log('Duplicate or error:', err.message);
        }
      }

      results.push({ 
        district, 
        success: true, 
        synced,
        total: records.length 
      });

    } catch (err) {
      results.push({ district, success: false, error: err.message });
    }
    
    // Rate limit: wait 2 seconds between requests
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  res.json({ results });
});

// Get all data from Appwrite
app.get('/api/data', async (req, res) => {
  try {
    const response = await databases.listDocuments(DATABASE_ID, COLLECTION_ID);
    res.json({ 
      success: true, 
      data: response.documents,
      total: response.total 
    });
  } catch (error) {
    console.error('Fetch error:', error.message);
    res.status(500).json({ 
      error: 'Failed to fetch data',
      message: error.message 
    });
  }
});

// Get data by district
app.get('/api/data/:district', async (req, res) => {
  const { district } = req.params;
  
  try {
    const response = await databases.listDocuments(
      DATABASE_ID, 
      COLLECTION_ID,
      [`district="${district}"`]
    );
    res.json({ 
      success: true, 
      data: response.documents,
      total: response.total 
    });
  } catch (error) {
    console.error('Fetch error:', error.message);
    res.status(500).json({ 
      error: 'Failed to fetch data',
      message: error.message 
    });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    environment: process.env.NODE_ENV || 'development'
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({ 
    message: 'MGNREGA Data Sync API',
    version: '1.0.0',
    endpoints: {
      health: 'GET /health',
      syncDistrict: 'POST /api/sync/:district',
      syncAll: 'POST /api/sync-all',
      getAllData: 'GET /api/data',
      getDistrictData: 'GET /api/data/:district'
    }
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`✅ Backend running on port ${PORT}`);
  console.log(`🌍 Environment: ${process.env.NODE_ENV || 'development'}`);
});
