// const mongoose = require('mongoose');
// const fs = require('fs');
// require('dotenv').config(); // Load .env if present

// const MONGODB_URI ='mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';

// // Define schemas
// const comboSchema = new mongoose.Schema({
//   comboWeight: Number,
//   products: [{ sku: String }]
// }, { collection: 'combos' });

// const productSchema = new mongoose.Schema({
//   sku: String,
//   grossWeight: Number
// }, { collection: 'products' });

// const Combo = mongoose.model('Combo', comboSchema);
// const Product = mongoose.model('Product', productSchema);

// async function main() {
//   try {
//     await mongoose.connect(MONGODB_URI, {
//         dbName: "CRM-Database",
//       useNewUrlParser: true,
//       useUnifiedTopology: true
//     });
//     console.log('✅ Connected to MongoDB');

//     const incorrectCombos = await Combo.aggregate([
//       { $unwind: "$products" },
//       {
//         $lookup: {
//           from: "products",
//           localField: "products.sku",
//           foreignField: "sku",
//           as: "productInfo"
//         }
//       },
//       { $unwind: "$productInfo" },
//       {
//         $group: {
//           _id: "$_id",
//           comboWeight: { $first: "$comboWeight" },
//           productSkus: { $push: "$products.sku" },
//           totalGrossWeight: { $sum: "$productInfo.grossWeight" }
//         }
//       },
//       {
//         $match: {
//           $expr: { $ne: ["$comboWeight", "$totalGrossWeight"] }
//         }
//       }
//     ]);

//     if (incorrectCombos.length === 0) {
//       console.log("✅ All combos have correct weights.");
//     } else {
//       const output = incorrectCombos.map(combo => ({
//         comboId: combo.comboSku,
//         actualWeight: combo.comboWeight,
//         calculatedWeight: combo.totalGrossWeight,
//         skus: combo.productSkus,
//         difference :(calculatedWeight - actualWeight).toFixed(5)
//       }));

//       fs.writeFileSync('incorrect_combos.json', JSON.stringify(output, null, 2));
//       console.log(`❌ Found ${output.length} incorrect combos. Output saved to incorrect_combos.json`);
//     }

//   } catch (error) {
//     console.error("❌ Error:", error);
//   } finally {
//     await mongoose.disconnect();
//   }
// }

// main();


const mongoose = require('mongoose');
const fs = require('fs');
require('dotenv').config();

const MONGODB_URI = process.env.MONGODB_URI ;

const comboSchema = new mongoose.Schema({
  comboWeight: Number,
  comboSku: String,
  products: [{ sku: String }]
}, { collection: 'combos' });

const productSchema = new mongoose.Schema({
  sku: String,
  grossWeight: Number
}, { collection: 'products' });

const Combo = mongoose.model('Combo', comboSchema);
const Product = mongoose.model('Product', productSchema);

async function main() {
  try {
    await mongoose.connect(MONGODB_URI, {
      dbName: "CRM-Database",
      useNewUrlParser: true
    });
    console.log('✅ Connected to MongoDB');

    const incorrectCombos = await Combo.aggregate([
      { $unwind: "$products" },
      {
        $lookup: {
          from: "products",
          localField: "products.sku",
          foreignField: "sku",
          as: "productInfo"
        }
      },
      { $unwind: "$productInfo" },
      {
        $group: {
          _id: "$_id",
          comboSku: { $first: "$comboSku" },
          comboWeight: { $first: "$comboWeight" },
          productSkus: { $push: "$products.sku" },
          totalGrossWeight: { $sum: "$productInfo.grossWeight" }
        }
      }
    ]);

    const filteredCombos = incorrectCombos
      .map(combo => {
        const diff = Math.abs(combo.totalGrossWeight - combo.comboWeight);
        return {
          comboSku: combo.comboSku,
          actualWeight: combo.comboWeight,
          calculatedWeight: combo.totalGrossWeight,
          difference: diff.toFixed(5),
          skus: combo.productSkus
        };
      })
      .filter(c => parseFloat(c.difference) > 0.00005); // ✅ Only include significant mismatches

    if (filteredCombos.length === 0) {
      console.log("✅ All combos have correct weights within allowed tolerance.");
    } else {
      fs.writeFileSync('incorrect_combos.json', JSON.stringify(filteredCombos, null, 2));
      console.log(`❌ Found ${filteredCombos.length} incorrect combos. Output saved to incorrect_combos.json`);
    }

  } catch (error) {
    console.error("❌ Error:", error);
  } finally {
    await mongoose.disconnect();
  }
}

main();
