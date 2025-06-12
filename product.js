// const mongoose = require("mongoose");
// const fs = require("fs");
// const path = require("path");
// const { Parser } = require("json2csv");

// const MONGO_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';

// mongoose.connect(MONGO_URI, {
//   useNewUrlParser: true,
//   useUnifiedTopology: true,
//   dbName: "CRM-Database",
// });

// // Define schema to allow all fields
// const productSchema = new mongoose.Schema({}, { strict: false });
// const Product = mongoose.model("Product", productSchema, "products");

// async function exportAndValidate() {
//   try {
//     const allProducts = await Product.find({}).lean();

//     const parentProducts = [];
//     const childProducts = [];
//     const childValidationResults = [];

//     const parentMap = new Map();

//     // First pass to collect all parent SKUs into a map
//     for (const product of allProducts) {
//       if (product.sku === product.parentSku && product.is_parent === true) {
//         parentProducts.push(product);
//         parentMap.set(product.sku, product);
//       }
//     }

//     // Now go through all products and classify children with validation
//     for (const product of allProducts) {
//       if (!(product.sku === product.parentSku && product.is_parent === true)) {
//         const parent = parentMap.get(product.parentSku);

//         let validationEntry = {
//           displayName: product.displayName,
//           sku: product.sku,
//           parentSku: product.parentSku,
//           itemQty: product.itemQty,
//           grossWeight: product.grossWeight,
//           is_parent: false,
//         };

//         if (parent) {
//           const expectedGrossWeight = (product.itemQty || 0) * (parent.grossWeight || 0);
//           const isCorrect = Math.abs((product.grossWeight || 0) - expectedGrossWeight) < 0.001;

//           validationEntry.parentDisplayName = parent.displayName;
//           validationEntry.parentGrossWeight = parent.grossWeight;
//           validationEntry.expectedGrossWeight = expectedGrossWeight.toFixed(3);
//           validationEntry.grossWeightCorrect = isCorrect;
//         } else {
//           validationEntry.parentDisplayName = "";
//           validationEntry.parentGrossWeight = "";
//           validationEntry.expectedGrossWeight = "";
//           validationEntry.grossWeightCorrect = "Parent SKU not found";
//         }

//         childProducts.push(product);
//         childValidationResults.push(validationEntry);
//       }
//     }

//     // CSV Field Definitions
//     const baseFields = [
//       "displayName",
//       "sku",
//       "parentSku",
//       "itemQty",
//       "grossWeight",
//       "is_parent",
//     ];

//     const validationFields = [
//       "displayName",
//       "sku",
//       "parentSku",
//       "itemQty",
//       "grossWeight",
//       "parentDisplayName",
//       "parentGrossWeight",
//       "expectedGrossWeight",
//       "grossWeightCorrect",
//     ];

//     const parserBase = new Parser({ fields: baseFields });
//     const parserValidation = new Parser({ fields: validationFields });

//     // Write CSVs
//     fs.writeFileSync("parent_productsG.csv", parserBase.parse(parentProducts));
//     fs.writeFileSync("child_productsG.csv", parserBase.parse(childProducts));
//     fs.writeFileSync("grossweight_validationG.csv", parserValidation.parse(childValidationResults));

//     console.log("✅ CSV files generated:");
//     console.log("- parent_products.csv");
//     console.log("- child_products.csv");
//     console.log("- grossweight_validation.csv");

//     mongoose.disconnect();
//   } catch (error) {
//     console.error("❌ Error:", error);
//     mongoose.disconnect();
//   }
// }

// exportAndValidate();
const mongoose = require("mongoose");
const fs = require("fs");
const path = require("path");
const { Parser } = require("json2csv");

const MONGO_URI = 'mongodb://developer:pmq21byndxhte3wul7i5fjav9r4o06kg@beta.mongoserver.ko-tech.in:27017/?authSource=admin&directConnection=true&tls=true';

mongoose.connect(MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  dbName: "CRM-Database",
});

const productSchema = new mongoose.Schema({}, { strict: false });
const Product = mongoose.model("Product", productSchema, "products");

async function exportWithValidation() {
  try {
    const allProducts = await Product.find({}).lean();

    const parentMap = new Map();
    const parentProducts = [];
    const childProducts = [];

    // First pass to build parent map
    for (const product of allProducts) {
      if (product.sku === product.parentSku && product.is_parent === true) {
        parentMap.set(product.sku, product);
      }
    }

    for (const product of allProducts) {
      const {
        displayName,
        sku,
        parentSku,
        itemQty,
        grossWeight,
        is_parent
      } = product;

      const entry = {
        displayName,
        sku,
        parentSku,
        itemQty,
        grossWeight,
        is_parent,
      };

      // Determine parent data
      const parent = parentMap.get(parentSku);
      if (parent) {
        entry.parentDisplayName = parent.displayName ;
        entry.parentGrossWeight = parent.grossWeight ;

        if (is_parent && sku === parentSku) {
          // Parent product itself
          entry.correctGrossWeight = parent.grossWeight;
          entry.isCorrectGrossWeight = true;
          parentProducts.push(entry);
        } else {
          // Child product
          const expectedGrossWeight = (itemQty ) * (parent.grossWeight);
          entry.correctGrossWeight = expectedGrossWeight;
          entry.isCorrectGrossWeight =
            Math.abs((grossWeight ) - expectedGrossWeight) < 0.001;
          childProducts.push(entry);
        }
      } else {
        // No parent found
        entry.parentDisplayName = "";
        entry.parentGrossWeight = "";
        entry.correctGrossWeight = "";
        entry.isCorrectGrossWeight = "Parent Not Found";
        childProducts.push(entry); // still push to child
      }
    }

    const fields = [
      "displayName",
      "sku",
      "parentSku",
      "itemQty",
      "grossWeight",
      "is_parent",
      "parentDisplayName",
      "parentGrossWeight",
      "correctGrossWeight",
      "isCorrectGrossWeight"
    ];

    const parser = new Parser({ fields });

    fs.writeFileSync("1parent_productsG.csv", parser.parse(parentProducts));
    fs.writeFileSync("1child_productsG.csv", parser.parse(childProducts));

    console.log("✅ CSV files generated:");
    console.log("- parent_products.csv");
    console.log("- child_products.csv");

    await mongoose.disconnect();
  } catch (error) {
    console.error("❌ Error:", error);
    await mongoose.disconnect();
  }
}

exportWithValidation();
