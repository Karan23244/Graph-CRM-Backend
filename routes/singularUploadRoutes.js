// routes/adjustUploadRoutes.js
const express = require("express");
const multer = require("multer");
const {
  handlesingularUpload,
} = require("../controllers/singularUploadController");

const router = express.Router();

const singularStorage = multer.diskStorage({
  destination: "uploads/singular", // separate folder
  filename: (_, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname.replace(/\s+/g, "_"));
  },
});

const singularUpload = multer({
  storage: singularStorage,
  limits: {
    fileSize: 1024 * 1024 * 800, // 800MB
    files: 20,
  },
});

router.post(
  "/singular-metrics",
  singularUpload.fields([{ name: "files", maxCount: 10 }]),
  handlesingularUpload,
);

module.exports = router;
