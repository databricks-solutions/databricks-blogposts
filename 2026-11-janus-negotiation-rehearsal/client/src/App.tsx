import { Routes, Route } from "react-router-dom";
import Layout from "@/components/Layout";
import Dashboard from "@/pages/Dashboard";
import MatterWorkspace from "@/pages/MatterWorkspace";
import GovernanceAudit from "@/pages/GovernanceAudit";

export default function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<Dashboard />} />
        <Route path="/matter/:id" element={<MatterWorkspace />} />
        <Route path="/governance" element={<GovernanceAudit />} />
      </Routes>
    </Layout>
  );
}
