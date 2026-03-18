import { useQuery } from "@tanstack/react-query"
import { getCompaniesForUser } from "@/services/companiesService"

export function useCompanies() {
  return useQuery({
    queryKey: ["companies"],
    queryFn: getCompaniesForUser,
  })
}
