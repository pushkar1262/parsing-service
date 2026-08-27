// ecosystem.config.js
module.exports = {
  apps: [
    {
      name: "parsing-service",
      script: "./.venv/bin/python",
      args: "-m work.main",
      interpreter: "none",

      cwd: "/home/user/Projects/eos/parsing-service",

      env: {
        PYTHONPATH: "src",
        PYTHONUNBUFFERED: "1",
      },

      autorestart: true,
      restart_delay: 5000,

      // Useful for a long-running consumer
      kill_timeout: 10000,
    },
  ],
};
