
const mongoose = require('mongoose');
const fs = require('fs');
require('dotenv').config();

// === CONFIG ===
const MONGODB_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true'

const JSON_PATH = './incorrect_combos.json'; // adjust path if needed

// === Schema (only required fields) ===
const comboSchema = new mongoose.Schema({
  comboSku: String,
  comboWeight: Number
}, { collection: 'combos' });

const Combo = mongoose.model('Combo', comboSchema);

async function updateComboWeights() {
  try {
    await mongoose.connect(MONGODB_URI, {
      dbName: "CRM-Database",
      useNewUrlParser: true
    });
    console.log('✅ Connected to MongoDB');

    const data = JSON.parse(fs.readFileSync(JSON_PATH, 'utf-8'));

    let updated = 0, notFound = 0;

    for (const entry of data) {
      const { comboSku, calculatedWeight } = entry;

      const result = await Combo.updateOne(
        { comboSku },
        { $set: { comboWeight: calculatedWeight } }
      );

      if (result.matchedCount > 0) {
        console.log(`✅ Updated comboSku ${comboSku} to weight ${calculatedWeight}`);
        updated++;
      } else {
        console.warn(`⚠️ Combo with comboSku ${comboSku} not found`);
        notFound++;
      }
    }

    console.log(`\n✔️ Update complete: ${updated} updated, ${notFound} not found.`);
  } catch (err) {
    console.error("❌ Error:", err);
  } finally {
    await mongoose.disconnect();
  }
}

updateComboWeights();
