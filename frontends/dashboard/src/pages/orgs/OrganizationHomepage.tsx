import { DatasetOverview } from "../../components/DatasetOverview";
import { GettingStartedDocsLinks } from "../../components/GettingStartedDocsLinks";
import { OnboardingSteps } from "../../components/OnboardingSteps";
import OrgUpdateAlert from "../../components/OrgUpdateAlert";
import { TrieveMaintenanceAlert } from "../../components/TrieveMaintenanceAlert";

export const OrganizationHomepage = () => {
  return (
    <div class="pb-8">
      {import.meta.env.VITE_MAINTENANCE_ON == "true" && (
        <TrieveMaintenanceAlert />
      )}
      <div class="h-1" />
      <OrgUpdateAlert />
      <div class="h-1" />
      <OnboardingSteps />
      <div class="h-1" />
      <DatasetOverview />
      <div class="h-6" />
      <GettingStartedDocsLinks />
    </div>
  );
};
