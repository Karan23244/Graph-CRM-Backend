// routes/adjustUploadRoutes.js
const express = require("express");
const multer = require("multer");
const { handleAdjustUpload } = require("../controllers/adjustUploadController");

const router = express.Router();

const adjustStorage = multer.diskStorage({
  destination: "uploads/adjust", // separate folder
  filename: (_, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname.replace(/\s+/g, "_"));
  },
});

const adjustUpload = multer({
  storage: adjustStorage,
  limits: {
    fileSize: 1024 * 1024 * 800, // 800MB
    files: 20,
  },
});

router.post(
  "/adjust-metrics",
  adjustUpload.fields([{ name: "adjustFiles", maxCount: 10 }]),
  handleAdjustUpload
);

module.exports = router;
