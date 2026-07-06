const express = require("express");
const multer = require("multer");
const { handleUpload } = require("../controllers/uploadController");

const router = express.Router();

const storage = multer.diskStorage({
  destination: "uploads",
  filename: (_, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname.replace(/\s+/g, "_"));
  },
});

const upload = multer({
  storage,
  limits: {
    fileSize: 2 * 1024 * 1024 * 1024, // 2 GB per file
    files: 10, // allow up to 10 files
  },
});

router.post(
  "/metrics",
  upload.fields([{ name: "files", maxCount: 10 }]),
  handleUpload,
);

module.exports = router;
